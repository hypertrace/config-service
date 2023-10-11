package org.hypertrace.config.service.store;

import static com.google.common.collect.Streams.zip;
import static org.hypertrace.config.service.ConfigServiceUtils.emptyConfig;
import static org.hypertrace.config.service.store.ConfigDocument.CONTEXT_FIELD_NAME;
import static org.hypertrace.config.service.store.ConfigDocument.RESOURCE_FIELD_NAME;
import static org.hypertrace.config.service.store.ConfigDocument.RESOURCE_NAMESPACE_FIELD_NAME;
import static org.hypertrace.config.service.store.ConfigDocument.TENANT_ID_FIELD_NAME;
import static org.hypertrace.config.service.store.ConfigDocument.VERSION_FIELD_NAME;

import com.google.protobuf.Value;
import com.typesafe.config.Config;
import io.grpc.Status;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.ConfigResource;
import org.hypertrace.config.service.ConfigResourceContext;
import org.hypertrace.config.service.ConfigServiceUtils;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse.UpsertedConfig;
import org.hypertrace.core.documentstore.CloseableIterator;
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
    Optional<ConfigDocument> previousConfigDoc = getLatestVersionConfigDoc(configResourceContext);
    Optional<ContextSpecificConfig> optionalPreviousConfig =
        previousConfigDoc.flatMap(this::convertToContextSpecificConfig);

    Key latestDocKey = new ConfigDocumentKey(configResourceContext);
    ConfigDocument latestConfigDocument =
        buildConfigDocument(configResourceContext, latestConfig, userId, previousConfigDoc);

    collection.upsert(latestDocKey, latestConfigDocument);
    return optionalPreviousConfig
        .map(previousConfig -> this.buildUpsertResult(latestConfigDocument, previousConfig))
        .orElseGet(() -> this.buildUpsertResult(latestConfigDocument));
  }

  private List<UpsertedConfig> writeConfigs(
      Map<ConfigResourceContext, Value> resourceContextValueMap, String userId) throws IOException {
    Map<ConfigResourceContext, Optional<ConfigDocument>> previousConfigDocs =
        getLatestVersionConfigDocs(resourceContextValueMap.keySet());
    Map<Key, Document> documentsToBeUpserted = new LinkedHashMap<>();
    previousConfigDocs.forEach(
        (key, value) -> {
          if (!resourceContextValueMap.containsKey(key)) {
            throw Status.INTERNAL.asRuntimeException();
          }
          documentsToBeUpserted.put(
              new ConfigDocumentKey(key),
              buildConfigDocument(key, resourceContextValueMap.get(key), userId, value));
        });

    boolean successfulBulkUpsertDocuments = collection.bulkUpsert(documentsToBeUpserted);
    if (successfulBulkUpsertDocuments) {
      return zip(
              previousConfigDocs.values().stream(),
              documentsToBeUpserted.values().stream(),
              (previousConfigDoc, documentToBeUpserted) ->
                  previousConfigDoc
                      .flatMap(this::convertToContextSpecificConfig)
                      .map(
                          previousConfig ->
                              this.buildUpsertResult(
                                  (ConfigDocument) documentToBeUpserted, previousConfig))
                      .orElseGet(
                          () -> this.buildUpsertResult((ConfigDocument) documentToBeUpserted)))
          .collect(Collectors.toUnmodifiableList());
    }
    return Collections.emptyList();
  }

  private ConfigDocument buildConfigDocument(
      ConfigResourceContext configResourceContext,
      Value latestConfig,
      String userId,
      Optional<ConfigDocument> previousConfigDoc) {
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
    return new ConfigDocument(
        configResourceContext.getConfigResource().getResourceName(),
        configResourceContext.getConfigResource().getResourceNamespace(),
        configResourceContext.getConfigResource().getTenantId(),
        configResourceContext.getContext(),
        newVersion,
        userId,
        latestConfig,
        creationTimestamp,
        updateTimestamp);
  }

  @Override
  public List<UpsertedConfig> writeAllConfigs(
      Map<ConfigResourceContext, Value> resourceContextValueMap, String userId) throws IOException {
    return this.writeConfigs(resourceContextValueMap, userId);
  }

  @Override
  public void deleteConfigs(Set<ConfigResourceContext> configResourceContexts) {
    if (configResourceContexts.isEmpty()) {
      return;
    }
    collection.delete(getConfigResourceContextsFilter(configResourceContexts));
  }

  @Override
  public ContextSpecificConfig getConfig(ConfigResourceContext configResourceContext)
      throws IOException {
    return getLatestVersionConfigDoc(configResourceContext)
        .flatMap(this::convertToContextSpecificConfig)
        .orElseGet(() -> emptyConfig(configResourceContext.getContext()));
  }

  @Override
  public List<ContextSpecificConfig> getConfigs(Set<ConfigResourceContext> configResourceContexts)
      throws IOException {
    Map<ConfigResourceContext, Optional<ConfigDocument>> configDocs =
        getLatestVersionConfigDocs(configResourceContexts);
    return configDocs.values().stream()
        .flatMap(Optional::stream)
        .map(this::convertToContextSpecificConfig)
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<ContextSpecificConfig> getAllConfigs(ConfigResource configResource)
      throws IOException {
    Query query = new Query();
    query.setFilter(this.getConfigResourceFilter(configResource));
    query.addOrderBy(new OrderBy(VERSION_FIELD_NAME, false));
    List<ContextSpecificConfig> contextSpecificConfigList = new ArrayList<>();
    Set<String> seenContexts = new HashSet<>();
    try (CloseableIterator<Document> documentIterator = collection.search(query)) {
      while (documentIterator.hasNext()) {
        String documentString = documentIterator.next().toJson();
        ConfigDocument configDocument = ConfigDocument.fromJson(documentString);
        String context = configDocument.getContext();
        if (seenContexts.add(context)) {
          convertToContextSpecificConfig(configDocument).ifPresent(contextSpecificConfigList::add);
        }
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

    try (CloseableIterator<Document> documentIterator = collection.search(query)) {
      if (documentIterator.hasNext()) {
        String documentString = documentIterator.next().toJson();
        return Optional.of(ConfigDocument.fromJson(documentString));
      }
    }
    return Optional.empty();
  }

  private Map<ConfigResourceContext, Optional<ConfigDocument>> getLatestVersionConfigDocs(
      Set<ConfigResourceContext> configResourceContexts) throws IOException {
    if (configResourceContexts.isEmpty()) {
      return Collections.emptyMap();
    }

    Filter filter = getConfigResourceContextsFilter(configResourceContexts);
    // build query
    Query query = new Query();
    query.setFilter(filter);
    query.setLimit(configResourceContexts.size());

    Map<ConfigResourceContext, Optional<ConfigDocument>> latestVersionConfigDocs =
        new LinkedHashMap<>();
    // initialize latestVersionConfigDocs
    for (ConfigResourceContext configResourceContext : configResourceContexts) {
      latestVersionConfigDocs.put(configResourceContext, Optional.empty());
    }

    // populate
    try (CloseableIterator<Document> documentIterator = collection.search(query)) {
      while (documentIterator.hasNext()) {
        String documentString = documentIterator.next().toJson();
        ConfigDocument configDocument = ConfigDocument.fromJson(documentString);
        latestVersionConfigDocs.put(
            buildConfigResourceContext(configDocument), Optional.of(configDocument));
      }
    }
    return latestVersionConfigDocs;
  }

  private Filter getConfigResourceContextsFilter(
      Set<ConfigResourceContext> configResourceContexts) {
    List<Filter> childFilters =
        configResourceContexts.stream()
            .map(this::getConfigResourceFieldContextFilter)
            .collect(Collectors.toUnmodifiableList());
    Filter configResourceFieldContextFilter = new Filter();
    configResourceFieldContextFilter.setOp(Filter.Op.OR);
    configResourceFieldContextFilter.setChildFilters(childFilters.toArray(Filter[]::new));
    Filter tenantIdFilter =
        Filter.eq(
            TENANT_ID_FIELD_NAME,
            configResourceContexts.iterator().next().getConfigResource().getTenantId());
    return tenantIdFilter.and(configResourceFieldContextFilter);
  }

  private Filter getConfigResourceContextFilter(ConfigResourceContext configResourceContext) {
    return this.getConfigResourceFilter(configResourceContext.getConfigResource())
        .and(Filter.eq(CONTEXT_FIELD_NAME, configResourceContext.getContext()));
  }

  private Filter getConfigResourceFieldContextFilter(ConfigResourceContext configResourceContext) {
    ConfigResource configResource = configResourceContext.getConfigResource();
    return Filter.eq(RESOURCE_FIELD_NAME, configResource.getResourceName())
        .and(Filter.eq(RESOURCE_NAMESPACE_FIELD_NAME, configResource.getResourceNamespace()))
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

  private ConfigResourceContext buildConfigResourceContext(ConfigDocument configDocument) {
    return new ConfigResourceContext(
        new ConfigResource(
            configDocument.getResourceName(),
            configDocument.getResourceNamespace(),
            configDocument.getTenantId()),
        configDocument.getContext());
  }
}
