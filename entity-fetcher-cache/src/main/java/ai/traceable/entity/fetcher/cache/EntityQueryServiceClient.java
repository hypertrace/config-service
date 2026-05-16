package ai.traceable.entity.fetcher.cache;

import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import ai.traceable.entity.fetcher.cache.StreamingAiEndpointMetadataProvider.ApiAiEndpointMetadataDetails;
import ai.traceable.entity.fetcher.cache.StreamingApiMappingProvider.HttpApiDetails;
import ai.traceable.entity.fetcher.cache.config.EntityQueryServiceConfig;
import ai.traceable.protection.data.context.v1.AiEndpointMetadata;
import ai.traceable.protection.data.context.v1.PromptAttributeKey;
import ai.traceable.protection.processing.common.v1.AttributeType;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc.EntityQueryServiceBlockingStub;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.v1.entitytype.EntityType;

@Slf4j
class EntityQueryServiceClient {
  private final EntityQueryServiceConfig entityQueryServiceConfig;
  private final EntityQueryServiceBlockingStub entityQueryServiceBlockingStub;
  private final long timeoutMillis;

  private static final Map<String, AttributeType> ATTRIBUTE_TYPE_MAPPING =
      Map.of(
          "header",
          AttributeType.ATTRIBUTE_TYPE_HEADER,
          "cookie",
          AttributeType.ATTRIBUTE_TYPE_COOKIE,
          "query",
          AttributeType.ATTRIBUTE_TYPE_QUERY_PARAM,
          "body",
          AttributeType.ATTRIBUTE_TYPE_BODY_PARAM);

  @Inject
  EntityQueryServiceClient(
      EntityQueryServiceConfig config,
      EntityQueryServiceBlockingStub entityQueryServiceBlockingStub) {
    this.entityQueryServiceConfig = config;
    this.entityQueryServiceBlockingStub = entityQueryServiceBlockingStub;
    this.timeoutMillis = config.getTimeout().toMillis();
  }

  public Stream<HttpApiDetails> getAllLearntHttpApiEndpoints(
      RequestContext requestContext, String serviceName, String environment) {
    EntityQueryRequest serviceEntityQueryRequest =
        buildLearntApiQueryRequest(serviceName, environment, ApiType.HTTP);
    return executeApiQueryRequest(requestContext, serviceName, serviceEntityQueryRequest);
  }

  public Stream<HttpApiDetails> getAllHttpApiEndpoints(
      RequestContext requestContext, String serviceName, String environment) {
    EntityQueryRequest serviceEntityQueryRequest =
        buildAllApiQueryRequest(serviceName, environment, ApiType.HTTP);
    return executeApiQueryRequest(requestContext, serviceName, serviceEntityQueryRequest);
  }

  public Stream<ApiAiEndpointMetadataDetails> getAllAiEndpointMetadata(
      RequestContext requestContext, String serviceName, String environment) {
    EntityQueryRequest queryRequest = buildAiEndpointMetadataQueryRequest(serviceName, environment);
    return executeAiEndpointMetadataQueryRequest(requestContext, serviceName, queryRequest);
  }

  private Stream<ApiAiEndpointMetadataDetails> executeAiEndpointMetadataQueryRequest(
      RequestContext requestContext, String serviceName, EntityQueryRequest queryRequest) {
    Iterator<ResultSetChunk> resultSetChunkIterator =
        requestContext.call(
            () ->
                entityQueryServiceBlockingStub
                    .withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .execute(queryRequest));

    if (!resultSetChunkIterator.hasNext()) {
      return Stream.empty();
    }

    int selectionCount = queryRequest.getSelectionCount();
    return Streams.stream(resultSetChunkIterator)
        .map(ResultSetChunk::getRowList)
        .flatMap(List::stream)
        .filter(row -> hasExpectedColCount(row, selectionCount, requestContext, serviceName))
        .map(
            row -> {
              String apiId = row.getColumn(0).getString();
              AiEndpointMetadata.Builder builder =
                  AiEndpointMetadata.newBuilder()
                      .addAllAssociatedAiModels(row.getColumn(1).getStringArrayList())
                      .addAllAssociatedAiVendors(row.getColumn(2).getStringArrayList())
                      .addAllPromptAttributeKeys(
                          parsePromptAttributeKeys(row.getColumn(3).getStringArrayList()));
              return new ApiAiEndpointMetadataDetails(apiId, builder.build());
            });
  }

  private List<PromptAttributeKey> parsePromptAttributeKeys(List<String> promptAttributeKeysRaw) {
    return promptAttributeKeysRaw.stream()
        .filter(raw -> raw != null && !raw.isEmpty())
        .map(raw -> raw.endsWith(",") ? raw.substring(0, raw.length() - 1) : raw)
        .map(
            raw -> {
              // Format: http.request.{attributeType}[.$.{jsonPath}]
              String[] parts = raw.split("\\.", 4);
              PromptAttributeKey.Builder builder = PromptAttributeKey.newBuilder();
              if (parts.length >= 3) {
                AttributeType attributeType = ATTRIBUTE_TYPE_MAPPING.get(parts[2]);
                if (attributeType != null) {
                  if (AttributeType.ATTRIBUTE_TYPE_BODY_PARAM.equals(attributeType)
                      && parts.length == 3) {
                    builder.setAttributeType(AttributeType.ATTRIBUTE_TYPE_BODY);
                  } else {
                    builder.setAttributeType(attributeType);
                  }
                }
                if (parts.length >= 4) {
                  String attributeKey = parts[3];
                  if (attributeKey.startsWith("$.")) {
                    attributeKey = attributeKey.substring(2);
                  }
                  builder.setAttributeKey(attributeKey);
                }
              }
              return builder.build();
            })
        .collect(Collectors.toList());
  }

  private EntityQueryRequest buildAiEndpointMetadataQueryRequest(
      String serviceName, String environment) {
    EntityQueryRequest.Builder requestBuilder =
        getInitializedApiQueryRequestBuilder(serviceName, environment, ApiType.HTTP);
    Filter isGenAiEndpointFilter =
        buildBooleanConstantEqualsFilter(
            entityQueryServiceConfig.getIsGenAiEndpointColumnName(), true);
    Filter finalFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(isGenAiEndpointFilter)
            .addChildFilter(requestBuilder.getFilter())
            .build();
    requestBuilder.setFilter(finalFilter);
    return requestBuilder
        .addSelection(
            buildSelectionExpression(entityQueryServiceConfig.getApiAssociatedAiModelsColumnName()))
        .addSelection(
            buildSelectionExpression(
                entityQueryServiceConfig.getApiAssociatedAiVendorsColumnName()))
        .addSelection(
            buildSelectionExpression(
                entityQueryServiceConfig.getApiPromptAttributeKeysColumnName()))
        .build();
  }

  private Stream<HttpApiDetails> executeApiQueryRequest(
      RequestContext requestContext, String serviceName, EntityQueryRequest queryRequest) {
    Iterator<ResultSetChunk> resultSetChunkIterator =
        requestContext.call(
            () ->
                entityQueryServiceBlockingStub
                    .withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .execute(queryRequest));

    if (!resultSetChunkIterator.hasNext()) {
      return Stream.empty();
    }

    int selectionCount = queryRequest.getSelectionCount();
    return Streams.stream(resultSetChunkIterator)
        .map(ResultSetChunk::getRowList)
        .flatMap(List::stream)
        .filter(row -> hasExpectedColCount(row, selectionCount, requestContext, serviceName))
        .map(
            row ->
                new HttpApiDetails(
                    row.getColumn(0).getString(),
                    row.getColumn(1).getString(),
                    row.getColumn(2).getStringArrayList()));
  }

  Map<ContextualKey<String>, Optional<ServiceIdentifierEntity>> getServiceEntities(
      final Iterable<? extends ContextualKey<String>> keys) {
    Iterator<? extends ContextualKey<String>> iterator = keys.iterator();
    if (!iterator.hasNext()) {
      return Collections.emptyMap();
    }
    final Map<String, ContextualKey<String>> serviceIdContextualKeyMap = new HashMap<>();
    RequestContext requestContext = null;
    while (iterator.hasNext()) {
      ContextualKey<String> key = iterator.next();
      if (requestContext == null) {
        requestContext = key.getContext();
      }
      serviceIdContextualKeyMap.put(key.getData(), key);
    }
    Set<String> serviceIds = serviceIdContextualKeyMap.keySet();
    EntityQueryRequest serviceEntityQueryRequest = buildServiceQueryRequest(serviceIds);
    Iterator<ResultSetChunk> resultSetChunkIterator =
        requestContext.call(
            () ->
                entityQueryServiceBlockingStub
                    .withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .execute(serviceEntityQueryRequest));

    Map<String, Optional<ServiceIdentifierEntity>> serviceEntitiesMap = new HashMap<>();
    serviceIds.forEach(serviceId -> serviceEntitiesMap.put(serviceId, Optional.empty()));
    if (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      chunk
          .getRowList()
          .forEach(
              row ->
                  serviceEntitiesMap.put(
                      row.getColumn(0).getString(),
                      Optional.of(
                          new ServiceIdentifierEntity(
                              row.getColumn(1).getString(),
                              row.getColumn(2).getString().isEmpty()
                                  ? Optional.empty()
                                  : Optional.of(row.getColumn(2).getString())))));
    }
    return serviceEntitiesMap.entrySet().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                entry -> serviceIdContextualKeyMap.get(entry.getKey()), Map.Entry::getValue));
  }

  Map<ContextualKey<String>, Optional<ApiIdentifierEntity>> getApiEntities(
      final Iterable<? extends ContextualKey<String>> keys) {
    Iterator<? extends ContextualKey<String>> iterator = keys.iterator();
    if (!iterator.hasNext()) {
      return Collections.emptyMap();
    }
    final Map<String, ContextualKey<String>> apiIdContextualKeyMap = new HashMap<>();
    RequestContext requestContext = null;
    while (iterator.hasNext()) {
      ContextualKey<String> key = iterator.next();
      if (requestContext == null) {
        requestContext = key.getContext();
      }
      apiIdContextualKeyMap.put(key.getData(), key);
    }
    Set<String> apiIds = apiIdContextualKeyMap.keySet();
    EntityQueryRequest apiEntityQueryRequest = buildApiQueryRequest(apiIds);
    Iterator<ResultSetChunk> resultSetChunkIterator =
        requestContext.call(
            () ->
                entityQueryServiceBlockingStub
                    .withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .execute(apiEntityQueryRequest));

    Map<String, Optional<ApiIdentifierEntity>> apiEntitiesMap = new HashMap<>();
    apiIds.forEach(apiId -> apiEntitiesMap.put(apiId, Optional.empty()));
    if (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      chunk
          .getRowList()
          .forEach(
              row ->
                  apiEntitiesMap.put(
                      row.getColumn(0).getString(),
                      Optional.of(
                          new ApiIdentifierEntity(
                              row.getColumn(0).getString(),
                              row.getColumn(1).getString(),
                              row.getColumn(2).getString(),
                              row.getColumn(3).getStringArrayList(),
                              Collections.emptyList(),
                              row.getColumn(4).getString(),
                              row.getColumn(5).getString()))));
    }
    return apiEntitiesMap.entrySet().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                entry -> apiIdContextualKeyMap.get(entry.getKey()), Map.Entry::getValue));
  }

  Map<ContextualKey<String>, Set<ApiIdentifierEntity>> getApiEntitiesHavingLabels(
      final Iterable<? extends ContextualKey<String>> keys) {
    Iterator<? extends ContextualKey<String>> iterator = keys.iterator();
    if (!iterator.hasNext()) {
      return Collections.emptyMap();
    }
    final Map<String, ContextualKey<String>> apiLabelIdContextualKeyMap = new HashMap<>();
    RequestContext requestContext = null;
    while (iterator.hasNext()) {
      ContextualKey<String> key = iterator.next();
      if (requestContext == null) {
        requestContext = key.getContext();
      }
      apiLabelIdContextualKeyMap.put(key.getData(), key);
    }
    Set<String> apiLabelIds = apiLabelIdContextualKeyMap.keySet();
    EntityQueryRequest apiEntityQueryRequest = buildApiQueryRequestWithLabels(apiLabelIds);
    Iterator<ResultSetChunk> resultSetChunkIterator =
        requestContext.call(
            () ->
                entityQueryServiceBlockingStub
                    .withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .execute(apiEntityQueryRequest));

    Map<String, Set<ApiIdentifierEntity>> apiEntitiesMap = new HashMap<>();
    apiLabelIds.forEach(
        apiLabelId -> apiEntitiesMap.computeIfAbsent(apiLabelId, k -> new HashSet<>()));
    if (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      chunk
          .getRowList()
          .forEach(
              row -> {
                ApiIdentifierEntity apiIdentifierEntity =
                    new ApiIdentifierEntity(
                        row.getColumn(0).getString(),
                        row.getColumn(1).getString(),
                        row.getColumn(2).getString(),
                        row.getColumn(3).getStringArrayList(),
                        row.getColumn(4).getStringArrayList(),
                        row.getColumn(5).getString(),
                        row.getColumn(6).getString());
                List<String> filteredApiLabelIds =
                    apiIdentifierEntity.getApiLabels().stream()
                        .filter(apiLabelIds::contains)
                        .collect(Collectors.toUnmodifiableList());
                filteredApiLabelIds.forEach(
                    apiLabelId -> apiEntitiesMap.get(apiLabelId).add(apiIdentifierEntity));
              });
    }
    return apiEntitiesMap.entrySet().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                entry -> apiLabelIdContextualKeyMap.get(entry.getKey()), Map.Entry::getValue));
  }

  private EntityQueryRequest buildServiceQueryRequest(Set<String> serviceIds) {
    return EntityQueryRequest.newBuilder()
        .setEntityType(EntityType.SERVICE.name())
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getServiceIdColumnName()))
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getServiceNameColumnName()))
        .addSelection(
            buildSelectionExpression(entityQueryServiceConfig.getServiceEnvironmentColumnName()))
        .setFilter(buildServiceIdFilter(serviceIds))
        .build();
  }

  private EntityQueryRequest buildApiQueryRequest(Set<String> apiIds) {
    return EntityQueryRequest.newBuilder()
        .setEntityType(EntityType.API.name())
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getApiIdColumnName()))
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getApiNameColumnName()))
        .addSelection(
            buildSelectionExpression(entityQueryServiceConfig.getApiUrlPatternColumnName()))
        .addSelection(
            buildSelectionExpression(
                entityQueryServiceConfig.getApiResolvedUrlPatternsColumnName()))
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getHttpMethodColumnName()))
        .addSelection(
            buildSelectionExpression(entityQueryServiceConfig.getApiServiceNameColumnName()))
        .setFilter(buildApiIdFilter(apiIds))
        .build();
  }

  private EntityQueryRequest buildApiQueryRequestWithLabels(Set<String> apiLabelIds) {
    return EntityQueryRequest.newBuilder()
        .setEntityType(EntityType.API.name())
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getApiIdColumnName()))
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getApiNameColumnName()))
        .addSelection(
            buildSelectionExpression(entityQueryServiceConfig.getApiUrlPatternColumnName()))
        .addSelection(
            buildSelectionExpression(
                entityQueryServiceConfig.getApiResolvedUrlPatternsColumnName()))
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getApiLabelsColumnName()))
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getHttpMethodColumnName()))
        .addSelection(
            buildSelectionExpression(entityQueryServiceConfig.getApiServiceNameColumnName()))
        .setFilter(buildApiLabelFilter(apiLabelIds))
        .build();
  }

  private EntityQueryRequest buildAllApiQueryRequest(
      String serviceId, String environment, ApiType apiType) {
    EntityQueryRequest.Builder requestBuilder =
        getInitializedApiQueryRequestBuilder(serviceId, environment, apiType);
    switch (apiType) {
      case SOAP:
      case XML_RPC:
      case HTTP:
        // HTTP, SOAP and XML_RPC APIs have httpMethod and resolvedUrlPatterns columns
        Expression httpMethodCol =
            buildSelectionExpression(entityQueryServiceConfig.getHttpMethodColumnName());
        Expression rUrlPattern =
            buildSelectionExpression(
                entityQueryServiceConfig.getApiResolvedUrlPatternsColumnName());
        return requestBuilder.addSelection(httpMethodCol).addSelection(rUrlPattern).build();
      case GRAPHQL:
      case GRPC:
      default:
        throw new IllegalArgumentException("Unsupported API type: " + apiType);
    }
  }

  private EntityQueryRequest buildLearntApiQueryRequest(
      String serviceId, String environment, ApiType apiType) {
    EntityQueryRequest.Builder requestBuilder =
        getInitializedLearntApiQueryRequestBuilder(serviceId, environment, apiType);
    switch (apiType) {
      case SOAP:
      case XML_RPC:
      case HTTP:
        // HTTP, SOAP and XML_RPC APIs have httpMethod and resolvedUrlPatterns columns
        Expression httpMethodCol =
            buildSelectionExpression(entityQueryServiceConfig.getHttpMethodColumnName());
        Expression rUrlPattern =
            buildSelectionExpression(
                entityQueryServiceConfig.getApiResolvedUrlPatternsColumnName());
        return requestBuilder.addSelection(httpMethodCol).addSelection(rUrlPattern).build();
      case GRAPHQL:
      case GRPC:
      default:
        throw new IllegalArgumentException("Unsupported API type: " + apiType);
    }
  }

  private EntityQueryRequest.Builder getInitializedApiQueryRequestBuilder(
      String serviceName, String environment, ApiType apiType) {
    Filter apiTypeFilter =
        buildStringLiteralConstantEqualsFilter(
            entityQueryServiceConfig.getApiTypeColumnName(), apiType.name());
    Filter environmentFilter =
        buildStringLiteralConstantEqualsFilter(
            entityQueryServiceConfig.getApiEnvironmentColumnName(), environment);
    Filter serviceNameFilter =
        buildStringLiteralConstantEqualsFilter(
            entityQueryServiceConfig.getApiServiceNameColumnName(), serviceName);

    return EntityQueryRequest.newBuilder()
        .setEntityType(EntityType.API.name())
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getApiIdColumnName()))
        .setFilter(
            Filter.newBuilder()
                .setOperator(Operator.AND)
                .addChildFilter(serviceNameFilter)
                .addChildFilter(environmentFilter)
                .addChildFilter(apiTypeFilter));
  }

  private EntityQueryRequest.Builder getInitializedLearntApiQueryRequestBuilder(
      String serviceName, String environment, ApiType apiType) {
    Filter apiTypeFilter =
        buildStringLiteralConstantEqualsFilter(
            entityQueryServiceConfig.getApiTypeColumnName(), apiType.name());
    Filter environmentFilter =
        buildStringLiteralConstantEqualsFilter(
            entityQueryServiceConfig.getApiEnvironmentColumnName(), environment);
    Filter serviceNameFilter =
        buildStringLiteralConstantEqualsFilter(
            entityQueryServiceConfig.getApiServiceNameColumnName(), serviceName);
    Filter isLearntFilter =
        Filter.newBuilder()
            .setLhs(
                buildSelectionExpression(entityQueryServiceConfig.getApiIsLearntStatusColumnName()))
            .setOperator(Operator.EQ)
            .setRhs(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setValueType(ValueType.BOOL).setBoolean(true))))
            .build();

    return EntityQueryRequest.newBuilder()
        .setEntityType(EntityType.API.name())
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getApiIdColumnName()))
        .setFilter(
            Filter.newBuilder()
                .setOperator(Operator.AND)
                .addChildFilter(isLearntFilter)
                .addChildFilter(serviceNameFilter)
                .addChildFilter(environmentFilter)
                .addChildFilter(apiTypeFilter));
  }

  private Expression buildSelectionExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName).build())
        .build();
  }

  private Filter buildServiceIdFilter(Set<String> serviceIds) {
    return Filter.newBuilder()
        .setLhs(buildSelectionExpression(entityQueryServiceConfig.getServiceIdColumnName()))
        .setOperator(Operator.IN)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setValueType(ValueType.STRING_ARRAY)
                                .addAllStringArray(serviceIds))))
        .build();
  }

  private Filter buildApiIdFilter(Set<String> apiIds) {
    return Filter.newBuilder()
        .setLhs(buildSelectionExpression(entityQueryServiceConfig.getApiIdColumnName()))
        .setOperator(Operator.IN)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setValueType(ValueType.STRING_ARRAY)
                                .addAllStringArray(apiIds))))
        .build();
  }

  private Filter buildApiLabelFilter(Set<String> apiLabelIds) {
    return Filter.newBuilder()
        .setLhs(buildSelectionExpression(entityQueryServiceConfig.getApiLabelsColumnName()))
        .setOperator(Operator.IN)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setValueType(ValueType.STRING_ARRAY)
                                .addAllStringArray(apiLabelIds))))
        .build();
  }

  private Filter buildStringLiteralConstantEqualsFilter(String columnName, String stringLiteral) {
    return Filter.newBuilder()
        .setLhs(buildSelectionExpression(columnName))
        .setOperator(Operator.EQ)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setValueType(ValueType.STRING)
                                .setString(stringLiteral))))
        .build();
  }

  private Filter buildBooleanConstantEqualsFilter(String columnName, boolean boolValue) {
    return Filter.newBuilder()
        .setLhs(buildSelectionExpression(columnName))
        .setOperator(Operator.EQ)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder().setValueType(ValueType.BOOL).setBoolean(boolValue))))
        .build();
  }

  private boolean hasExpectedColCount(
      Row row, int expectedColCount, RequestContext requestContext, String serviceId) {
    if (row.getColumnList().size() != expectedColCount) {
      if (log.isDebugEnabled()) {
        log.debug(
            "The number of cols in the row ({}) is not equal to the number of selections {} for service-id {} for requestContext {}",
            row,
            expectedColCount,
            serviceId,
            requestContext);
      }
      return false;
    }
    return true;
  }

  enum ApiType {
    HTTP,
    SOAP,
    XML_RPC,
    GRPC,
    GRAPHQL
  }
}
