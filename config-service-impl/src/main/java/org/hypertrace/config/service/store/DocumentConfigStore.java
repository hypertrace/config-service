package org.hypertrace.config.service.store;

import static com.google.common.collect.Streams.zip;
import static org.hypertrace.config.service.store.ConfigDocument.CONTEXT_FIELD_NAME;
import static org.hypertrace.config.service.store.ConfigDocument.CREATION_TIMESTAMP_FIELD_NAME;
import static org.hypertrace.config.service.store.ConfigDocument.RESOURCE_FIELD_NAME;
import static org.hypertrace.config.service.store.ConfigDocument.RESOURCE_NAMESPACE_FIELD_NAME;
import static org.hypertrace.config.service.store.ConfigDocument.TENANT_ID_FIELD_NAME;
import static org.hypertrace.core.documentstore.Filter.Op.OR;

import com.google.common.collect.Maps;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.ConfigResource;
import org.hypertrace.config.service.ConfigResourceContext;
import org.hypertrace.config.service.ConfigServiceUtils;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.SortBy;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse.UpsertedConfig;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.UpdateResult;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.operators.SortOrder;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.model.options.QueryOptions;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;

/** Document store which stores and serves the configurations. */
@Slf4j
public class DocumentConfigStore implements ConfigStore {
  static final String CONFIGURATIONS_COLLECTION = "configurations";
  private final Clock clock;

  private final Datastore datastore;
  private final Collection collection;
  private final FilterBuilder filterBuilder;
  private final FilterExpressionBuilder filterExpressionBuilder;

  public DocumentConfigStore(Clock clock, Datastore datastore) {
    this.clock = clock;
    this.datastore = datastore;
    this.collection = this.datastore.getCollection(CONFIGURATIONS_COLLECTION);
    this.filterBuilder = new FilterBuilder();
    this.filterExpressionBuilder = new FilterExpressionBuilder();
  }

  @Override
  public UpsertedConfig writeConfig(
      ConfigResourceContext configResourceContext,
      String lastUpdatedUserId,
      UpsertConfigRequest request,
      String lastUpdatedUserEmail)
      throws IOException {
    Optional<ConfigDocument> previousConfigDoc = getConfigDocument(configResourceContext);
    Optional<ContextSpecificConfig> optionalPreviousConfig =
        previousConfigDoc.flatMap(this::convertToContextSpecificConfig);

    // reject create config with condition
    if (optionalPreviousConfig.isEmpty() && request.hasUpsertCondition()) {
      throw Status.FAILED_PRECONDITION
          .withDescription("No upsert condition required for creating config")
          .asRuntimeException();
    }

    Key latestDocKey = new ConfigDocumentKey(configResourceContext);
    ConfigDocument latestConfigDocument =
        buildConfigDocument(
            configResourceContext,
            request.getConfig(),
            lastUpdatedUserId,
            previousConfigDoc,
            lastUpdatedUserEmail);

    if (request.hasUpsertCondition()) {
      Filter filter = this.filterBuilder.buildDocStoreFilter(request.getUpsertCondition());
      UpdateResult updateResult = collection.update(latestDocKey, latestConfigDocument, filter);
      if (updateResult.getUpdatedCount() <= 0) {
        throw Status.FAILED_PRECONDITION
            .withDescription("Update failed because upsert condition did not match given record")
            .asRuntimeException();
      }
    } else {
      collection.upsert(latestDocKey, latestConfigDocument);
    }

    return optionalPreviousConfig
        .map(previousConfig -> this.buildUpsertResult(latestConfigDocument, previousConfig))
        .orElseGet(() -> this.buildUpsertResult(latestConfigDocument));
  }

  private List<UpsertedConfig> writeConfigs(
      Map<ConfigResourceContext, Value> resourceContextValueMap, String userId, String userEmail)
      throws IOException {
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
              buildConfigDocument(key, resourceContextValueMap.get(key), userId, value, userEmail));
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
      String lastUpdatedUserId,
      Optional<ConfigDocument> previousConfigDoc,
      String lastUpdatedUserEmail) {
    long updateTimestamp = clock.millis();
    long creationTimestamp =
        previousConfigDoc
            .filter(configDocument -> !ConfigServiceUtils.isNull(configDocument.getConfig()))
            .map(ConfigDocument::getCreationTimestamp)
            .orElse(updateTimestamp);
    // Preserve creator email on updates, set on creates
    String createdByUserEmail =
        previousConfigDoc.map(ConfigDocument::getCreatedByUserEmail).orElse(lastUpdatedUserEmail);
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
        lastUpdatedUserId,
        lastUpdatedUserEmail,
        createdByUserEmail,
        latestConfig,
        creationTimestamp,
        updateTimestamp);
  }

  @Override
  public List<UpsertedConfig> writeAllConfigs(
      Map<ConfigResourceContext, Value> resourceContextValueMap, String userId, String userEmail)
      throws IOException {
    return this.writeConfigs(resourceContextValueMap, userId, userEmail);
  }

  @Override
  public void deleteConfigs(java.util.Collection<ConfigResourceContext> configResourceContexts) {
    if (configResourceContexts.isEmpty()) {
      return;
    }
    collection.delete(buildConfigResourceContextsFilter(configResourceContexts));
  }

  @Override
  public Optional<ContextSpecificConfig> getConfig(ConfigResourceContext configResourceContext)
      throws IOException {
    return getConfigDocument(configResourceContext).flatMap(this::convertToContextSpecificConfig);
  }

  @Override
  public Map<ConfigResourceContext, ContextSpecificConfig> getContextConfigs(
      java.util.Collection<ConfigResourceContext> configResourceContexts) throws IOException {
    return Maps.filterValues(
        Maps.transformValues(
            getLatestVersionConfigDocs(configResourceContexts),
            doc -> doc.flatMap(this::convertToContextSpecificConfig).orElse(null)),
        Objects::nonNull);
  }

  @Override
  public List<ContextSpecificConfig> getAllConfigs(
      ConfigResource configResource,
      org.hypertrace.config.service.v1.Filter filter,
      org.hypertrace.config.service.v1.Pagination pagination,
      List<SortBy> sortByList)
      throws IOException {

    Query query = buildQuery(configResource, filter, pagination, sortByList);
    List<ContextSpecificConfig> configList = new ArrayList<>();
    Set<String> seenContexts = new HashSet<>();

    try (CloseableIterator<Document> documentIterator =
        collection.query(query, QueryOptions.DEFAULT_QUERY_OPTIONS)) {
      while (documentIterator.hasNext()) {
        processDocument(documentIterator.next(), seenContexts, configList);
      }
    }
    return configList;
  }

  @Override
  public long getMatchingConfigsCount(
      ConfigResource configResource, org.hypertrace.config.service.v1.Filter filter) {
    Query query =
        buildQuery(
            configResource,
            filter,
            org.hypertrace.config.service.v1.Pagination.getDefaultInstance(),
            Collections.emptyList());
    return collection.count(query);
  }

  private Query buildQuery(
      ConfigResource configResource,
      @NonNull org.hypertrace.config.service.v1.Filter filter,
      @NonNull org.hypertrace.config.service.v1.Pagination pagination,
      List<SortBy> sortByList) {

    FilterTypeExpression combinedFilter = getCombinedFilter(configResource, filter);
    Query.QueryBuilder queryBuilder = Query.builder().setFilter(combinedFilter);
    if (!pagination.equals(org.hypertrace.config.service.v1.Pagination.getDefaultInstance())) {
      queryBuilder.setPagination(
          Pagination.builder().offset(pagination.getOffset()).limit(pagination.getLimit()).build());
    }
    if (!sortByList.isEmpty()) {
      sortByList.forEach(
          sortBy ->
              queryBuilder.addSort(
                  IdentifierExpression.of(
                      ConfigServiceUtils.buildConfigFieldPath(
                          sortBy.getSelection().getConfigJsonPath())),
                  convertSortOrder(sortBy)));
    } else {
      queryBuilder.addSort(IdentifierExpression.of(CREATION_TIMESTAMP_FIELD_NAME), SortOrder.DESC);
    }
    return queryBuilder.build();
  }

  private FilterTypeExpression getCombinedFilter(
      ConfigResource configResource,
      @NonNull org.hypertrace.config.service.v1.Filter additionalFilter) {

    FilterTypeExpression resourceFilter = getConfigResourceFilterTypeExpression(configResource);
    if (additionalFilter.equals(org.hypertrace.config.service.v1.Filter.getDefaultInstance())) {
      return resourceFilter;
    }

    FilterTypeExpression docStoreFilter =
        filterExpressionBuilder.buildFilterTypeExpression(additionalFilter);
    return LogicalExpression.and(resourceFilter, docStoreFilter);
  }

  @SneakyThrows
  private void processDocument(
      Document document, Set<String> seenContexts, List<ContextSpecificConfig> configList) {

    ConfigDocument configDocument = ConfigDocument.fromJson(document.toJson());
    String context = configDocument.getContext();

    if (seenContexts.add(context)) {
      convertToContextSpecificConfig(configDocument).ifPresent(configList::add);
    }
  }

  private static SortOrder convertSortOrder(SortBy sortBy) {
    switch (sortBy.getSortOrder()) {
      case SORT_ORDER_DESC:
        return SortOrder.DESC;
      case SORT_ORDER_ASC:
      default:
        return SortOrder.ASC;
    }
  }

  @Override
  public boolean healthCheck() {
    return datastore.healthCheck();
  }

  private Optional<ConfigDocument> getConfigDocument(ConfigResourceContext configResourceContext)
      throws IOException {
    Query query =
        Query.builder()
            .setFilter(getConfigResourceContextFilterTypeExpression(configResourceContext))
            .build();

    try (CloseableIterator<Document> documentIterator =
        collection.query(query, QueryOptions.DEFAULT_QUERY_OPTIONS)) {
      if (documentIterator.hasNext()) {
        String documentString = documentIterator.next().toJson();
        return Optional.of(ConfigDocument.fromJson(documentString));
      }
    }
    return Optional.empty();
  }

  private Map<ConfigResourceContext, Optional<ConfigDocument>> getLatestVersionConfigDocs(
      java.util.Collection<ConfigResourceContext> configResourceContexts) throws IOException {
    if (configResourceContexts.isEmpty()) {
      return Collections.emptyMap();
    }

    FilterTypeExpression filter =
        buildConfigResourceContextsFilterTypeExpression(configResourceContexts);
    // build query
    Query query =
        Query.builder()
            .setFilter(filter)
            .setPagination(
                Pagination.builder().offset(0).limit(configResourceContexts.size()).build())
            .build();

    Map<ConfigResourceContext, Optional<ConfigDocument>> latestVersionConfigDocs =
        new LinkedHashMap<>();
    // initialize latestVersionConfigDocs
    for (ConfigResourceContext configResourceContext : configResourceContexts) {
      latestVersionConfigDocs.put(configResourceContext, Optional.empty());
    }

    // populate
    try (CloseableIterator<Document> documentIterator =
        collection.query(query, QueryOptions.DEFAULT_QUERY_OPTIONS)) {
      while (documentIterator.hasNext()) {
        String documentString = documentIterator.next().toJson();
        ConfigDocument configDocument = ConfigDocument.fromJson(documentString);
        latestVersionConfigDocs.put(
            buildConfigResourceContext(configDocument), Optional.of(configDocument));
      }
    }
    return latestVersionConfigDocs;
  }

  private Filter buildConfigResourceContextsFilter(
      java.util.Collection<ConfigResourceContext> configResourceContexts) {
    if (configResourceContexts.isEmpty()) {
      throw new RuntimeException("Config resource contexts cannot be empty");
    }
    List<Filter> childFilters =
        configResourceContexts.stream()
            .map(this::getConfigResourceFieldContextFilter)
            .collect(Collectors.toUnmodifiableList());
    Filter configResourceFieldContextFilter = new Filter();
    configResourceFieldContextFilter.setOp(OR);
    configResourceFieldContextFilter.setChildFilters(childFilters.toArray(Filter[]::new));
    Filter tenantIdFilter =
        Filter.eq(
            TENANT_ID_FIELD_NAME,
            configResourceContexts.stream()
                .findFirst()
                .orElseThrow()
                .getConfigResource()
                .getTenantId());
    return tenantIdFilter.and(configResourceFieldContextFilter);
  }

  private FilterTypeExpression buildConfigResourceContextsFilterTypeExpression(
      java.util.Collection<ConfigResourceContext> configResourceContexts) {
    if (configResourceContexts.isEmpty()) {
      throw new RuntimeException("Config resource contexts cannot be empty");
    }
    List<FilterTypeExpression> childFilters =
        configResourceContexts.stream()
            .map(this::getConfigResourceFieldContextFilterTypeExpression)
            .collect(Collectors.toUnmodifiableList());
    FilterTypeExpression configResourceFieldContextFilter;
    if (childFilters.size() == 1) {
      configResourceFieldContextFilter = childFilters.get(0);
    } else {
      configResourceFieldContextFilter = LogicalExpression.or(childFilters);
    }
    FilterTypeExpression tenantIdFilter =
        RelationalExpression.of(
            IdentifierExpression.of(TENANT_ID_FIELD_NAME),
            RelationalOperator.EQ,
            ConstantExpression.of(
                configResourceContexts.stream()
                    .findFirst()
                    .orElseThrow()
                    .getConfigResource()
                    .getTenantId()));
    return LogicalExpression.and(tenantIdFilter, configResourceFieldContextFilter);
  }

  private FilterTypeExpression getConfigResourceContextFilterTypeExpression(
      ConfigResourceContext configResourceContext) {
    ConfigResource configResource = configResourceContext.getConfigResource();
    return LogicalExpression.and(
        List.of(
            RelationalExpression.of(
                IdentifierExpression.of(RESOURCE_FIELD_NAME),
                RelationalOperator.EQ,
                ConstantExpression.of(configResource.getResourceName())),
            RelationalExpression.of(
                IdentifierExpression.of(RESOURCE_NAMESPACE_FIELD_NAME),
                RelationalOperator.EQ,
                ConstantExpression.of(configResource.getResourceNamespace())),
            RelationalExpression.of(
                IdentifierExpression.of(TENANT_ID_FIELD_NAME),
                RelationalOperator.EQ,
                ConstantExpression.of(configResource.getTenantId())),
            RelationalExpression.of(
                IdentifierExpression.of(CONTEXT_FIELD_NAME),
                RelationalOperator.EQ,
                ConstantExpression.of(configResourceContext.getContext()))));
  }

  private Filter getConfigResourceFieldContextFilter(ConfigResourceContext configResourceContext) {
    ConfigResource configResource = configResourceContext.getConfigResource();
    return Filter.eq(RESOURCE_FIELD_NAME, configResource.getResourceName())
        .and(Filter.eq(RESOURCE_NAMESPACE_FIELD_NAME, configResource.getResourceNamespace()))
        .and(Filter.eq(CONTEXT_FIELD_NAME, configResourceContext.getContext()));
  }

  private FilterTypeExpression getConfigResourceFieldContextFilterTypeExpression(
      ConfigResourceContext configResourceContext) {
    ConfigResource configResource = configResourceContext.getConfigResource();
    return LogicalExpression.and(
        List.of(
            RelationalExpression.of(
                IdentifierExpression.of(RESOURCE_FIELD_NAME),
                RelationalOperator.EQ,
                ConstantExpression.of(configResource.getResourceName())),
            RelationalExpression.of(
                IdentifierExpression.of(RESOURCE_NAMESPACE_FIELD_NAME),
                RelationalOperator.EQ,
                ConstantExpression.of(configResource.getResourceNamespace())),
            RelationalExpression.of(
                IdentifierExpression.of(CONTEXT_FIELD_NAME),
                RelationalOperator.EQ,
                ConstantExpression.of(configResourceContext.getContext()))));
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
            .setCreatedByEmail(configDocument.getCreatedByUserEmail())
            .setLastUpdatedByEmail(configDocument.getLastUpdatedUserEmail())
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
        .setCreatedByEmail(configDocument.getCreatedByUserEmail())
        .setLastUpdatedByEmail(configDocument.getLastUpdatedUserEmail())
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

  private FilterTypeExpression getConfigResourceFilterTypeExpression(
      ConfigResource configResource) {
    return LogicalExpression.and(
        List.of(
            RelationalExpression.of(
                IdentifierExpression.of(RESOURCE_FIELD_NAME),
                RelationalOperator.EQ,
                ConstantExpression.of(configResource.getResourceName())),
            RelationalExpression.of(
                IdentifierExpression.of(RESOURCE_NAMESPACE_FIELD_NAME),
                RelationalOperator.EQ,
                ConstantExpression.of(configResource.getResourceNamespace())),
            RelationalExpression.of(
                IdentifierExpression.of(TENANT_ID_FIELD_NAME),
                RelationalOperator.EQ,
                ConstantExpression.of(configResource.getTenantId()))));
  }
}
