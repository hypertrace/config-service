package org.hypertrace.config.service.store;

import static org.hypertrace.config.service.ConfigServiceUtils.emptyConfig;
import static org.hypertrace.config.service.store.ConfigDocument.CONTEXT_FIELD_NAME;
import static org.hypertrace.config.service.store.ConfigDocument.RESOURCE_FIELD_NAME;
import static org.hypertrace.config.service.store.ConfigDocument.RESOURCE_NAMESPACE_FIELD_NAME;
import static org.hypertrace.config.service.store.ConfigDocument.TENANT_ID_FIELD_NAME;
import static org.hypertrace.config.service.store.ConfigDocument.VERSION_FIELD_NAME;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.Value;
import com.typesafe.config.Config;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.ConfigResource;
import org.hypertrace.config.service.ConfigResourceContext;
import org.hypertrace.config.service.ConfigServiceUtils;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse.UpsertedConfig;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.OrderBy;
import org.hypertrace.core.documentstore.Query;

/** Document store which stores and serves the configurations. */
@Slf4j
public class DocumentConfigStore implements ConfigStore {

  static final String DOC_STORE_CONFIG_KEY = "document.store";
  static final String DATA_STORE_TYPE = "dataStoreType";
  static final String CONFIGURATIONS_COLLECTION = "configurations";

  private final LoadingCache<ConfigResourceContext, Object> configResourceContextLocks =
      CacheBuilder.newBuilder()
          .expireAfterAccess(10, TimeUnit.MINUTES) // max lock time ever expected
          .maximumSize(1000)
          .build(CacheLoader.from(Object::new));
  private final Clock clock;

  private Datastore datastore;
  private Collection collection;

  public DocumentConfigStore() {
    this(Clock.systemUTC());
  }

  DocumentConfigStore(Clock clock) {
    this.clock = clock;
  }

  @Override
  public void init(Config config) {
    datastore = initDataStore(config);
    this.collection = this.datastore.getCollection(CONFIGURATIONS_COLLECTION);
  }

  private Datastore initDataStore(Config config) {
    Config docStoreConfig = config.getConfig(DOC_STORE_CONFIG_KEY);
    String dataStoreType = docStoreConfig.getString(DATA_STORE_TYPE);
    Config dataStoreConfig = docStoreConfig.getConfig(dataStoreType);
    return DatastoreProvider.getDatastore(dataStoreType, dataStoreConfig);
  }

  @Override
  public UpsertedConfig writeConfig(
      ConfigResourceContext configResourceContext, String userId, Value latestConfig)
      throws IOException {
    // Synchronization is required across different threads trying to write the latest config
    // for the same resource into the document store
    synchronized (configResourceContextLocks.getUnchecked(configResourceContext)) {
      Optional<ConfigDocument> previousConfigDoc = getLatestVersionConfigDoc(configResourceContext);
      Optional<ContextSpecificConfig> optionalPreviousConfig =
          previousConfigDoc.flatMap(this::convertToContextSpecificConfig);
      long updateTimestamp = clock.millis();
      long creationTimestamp =
          previousConfigDoc
              .filter(configDocument -> !ConfigServiceUtils.isNull(configDocument.getConfig()))
              .map(ConfigDocument::getCreationTimestamp)
              .orElse(updateTimestamp);
      long newVersion =
          previousConfigDoc
              .map(ConfigDocument::getConfigVersion)
              .map(previousVersion -> previousVersion + 1)
              .orElse(1L);
      Key latestDocKey = new ConfigDocumentKey(configResourceContext, newVersion);
      ConfigDocument latestConfigDocument =
          new ConfigDocument(
              configResourceContext.getConfigResource().getResourceName(),
              configResourceContext.getConfigResource().getResourceNamespace(),
              configResourceContext.getConfigResource().getTenantId(),
              configResourceContext.getContext(),
              newVersion,
              userId,
              latestConfig,
              creationTimestamp,
              updateTimestamp);
      collection.upsert(latestDocKey, latestConfigDocument);
      return optionalPreviousConfig
          .map(previousConfig -> this.buildUpsertResult(latestConfigDocument, previousConfig))
          .orElseGet(() -> this.buildUpsertResult(latestConfigDocument));
    }
  }

  @Override
  public List<UpsertedConfig> writeAllConfigs(
      Map<ConfigResourceContext, Value> resourceContextValueMap, String userId) throws IOException {
    // TODO keep pushing this down into doc store
    List<UpsertedConfig> list = new ArrayList<>(resourceContextValueMap.size());
    for (Entry<ConfigResourceContext, Value> entry : resourceContextValueMap.entrySet()) {
      list.add(this.writeConfig(entry.getKey(), userId, entry.getValue()));
    }
    return Collections.unmodifiableList(list);
  }

  @Override
  public ContextSpecificConfig getConfig(ConfigResourceContext configResourceContext)
      throws IOException {
    return getLatestVersionConfigDoc(configResourceContext)
        .flatMap(this::convertToContextSpecificConfig)
        .orElseGet(() -> emptyConfig(configResourceContext.getContext()));
  }

  @Override
  public List<ContextSpecificConfig> getAllConfigs(ConfigResource configResource)
      throws IOException {
    Query query = new Query();
    query.setFilter(this.getConfigResourceFilter(configResource));
    query.addOrderBy(new OrderBy(VERSION_FIELD_NAME, false));
    Iterator<Document> documentIterator = collection.search(query);
    List<ContextSpecificConfig> contextSpecificConfigList = new ArrayList<>();
    Set<String> seenContexts = new HashSet<>();
    while (documentIterator.hasNext()) {
      String documentString = documentIterator.next().toJson();
      ConfigDocument configDocument = ConfigDocument.fromJson(documentString);
      String context = configDocument.getContext();
      if (seenContexts.add(context)) {
        convertToContextSpecificConfig(configDocument).ifPresent(contextSpecificConfigList::add);
      }
    }
    Collections.sort(
        contextSpecificConfigList,
        Comparator.comparingLong(ContextSpecificConfig::getCreationTimestamp).reversed());
    return contextSpecificConfigList;
  }

  @Override
  public boolean healthCheck() {
    return datastore.healthCheck();
  }

  private Optional<ConfigDocument> getLatestVersionConfigDoc(
      ConfigResourceContext configResourceContext) throws IOException {
    Query query = new Query();
    query.setFilter(getConfigResourceContextFilter(configResourceContext));
    query.addOrderBy(new OrderBy(VERSION_FIELD_NAME, false));
    query.setLimit(1);

    Iterator<Document> documentIterator = collection.search(query);
    if (documentIterator.hasNext()) {
      String documentString = documentIterator.next().toJson();
      return Optional.of(ConfigDocument.fromJson(documentString));
    }
    return Optional.empty();
  }

  private Filter getConfigResourceContextFilter(ConfigResourceContext configResourceContext) {
    return this.getConfigResourceFilter(configResourceContext.getConfigResource())
        .and(Filter.eq(CONTEXT_FIELD_NAME, configResourceContext.getContext()));
  }

  private Filter getConfigResourceFilter(ConfigResource configResource) {
    return Filter.eq(RESOURCE_FIELD_NAME, configResource.getResourceName())
        .and(Filter.eq(RESOURCE_NAMESPACE_FIELD_NAME, configResource.getResourceNamespace()))
        .and(Filter.eq(TENANT_ID_FIELD_NAME, configResource.getTenantId()));
  }

  private Optional<ContextSpecificConfig> convertToContextSpecificConfig(
      ConfigDocument configDocument) {
    if (ConfigServiceUtils.isNull(configDocument.getConfig())) {
      return Optional.empty();
    }
    return Optional.of(
        ContextSpecificConfig.newBuilder()
            .setConfig(configDocument.getConfig())
            .setContext(configDocument.getContext())
            .setCreationTimestamp(configDocument.getCreationTimestamp())
            .setUpdateTimestamp(configDocument.getUpdateTimestamp())
            .build());
  }

  private UpsertedConfig buildUpsertResult(
      ConfigDocument configDocument, ContextSpecificConfig existingConfig) {
    return this.buildUpsertResult(configDocument).toBuilder()
        .setPrevConfig(existingConfig.getConfig())
        .build();
  }

  private UpsertedConfig buildUpsertResult(ConfigDocument configDocument) {
    return UpsertedConfig.newBuilder()
        .setConfig(configDocument.getConfig())
        .setContext(configDocument.getContext())
        .setCreationTimestamp(configDocument.getCreationTimestamp())
        .setUpdateTimestamp(configDocument.getUpdateTimestamp())
        .build();
  }
}
