package ai.traceable.entity.fetcher.cache;

import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import ai.traceable.entity.fetcher.cache.config.EntityQueryServiceConfig;
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
import lombok.NonNull;
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
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.v1.entitytype.EntityType;

class EntityQueryServiceClient {
  private final EntityQueryServiceConfig entityQueryServiceConfig;
  private final EntityQueryServiceBlockingStub entityQueryServiceBlockingStub;
  private final long timeoutMillis;

  @Inject
  EntityQueryServiceClient(
      EntityQueryServiceConfig config,
      EntityQueryServiceBlockingStub entityQueryServiceBlockingStub) {
    this.entityQueryServiceConfig = config;
    this.entityQueryServiceBlockingStub = entityQueryServiceBlockingStub;
    this.timeoutMillis = config.getTimeout().toMillis();
  }

  Optional<ServiceIdentifierEntity> getServiceEntity(
      @NonNull final ContextualKey<String> serviceIdContextualKey) {
    EntityQueryRequest serviceEntityQueryRequest =
        buildServiceQueryRequest(serviceIdContextualKey.getData());
    Iterator<ResultSetChunk> resultSetChunkIterator =
        serviceIdContextualKey.callInContext(
            () ->
                entityQueryServiceBlockingStub
                    .withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .execute(serviceEntityQueryRequest));

    if (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      if (!chunk.getRowList().isEmpty()) {
        return Optional.of(
            new ServiceIdentifierEntity(
                chunk.getRow(0).getColumn(1).getString(),
                chunk.getRow(0).getColumn(2).getString().isEmpty()
                    ? Optional.empty()
                    : Optional.of(chunk.getRow(0).getColumn(2).getString())));
      }
    }
    return Optional.empty();
  }

  public Map<ContextualKey<String>, Optional<ApiIdentifierEntity>> getApiEntities(
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
                              Collections.emptyList()))));
    }
    return apiEntitiesMap.entrySet().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                entry -> apiIdContextualKeyMap.get(entry.getKey()), Map.Entry::getValue));
  }

  public Map<ContextualKey<String>, Set<ApiIdentifierEntity>> getApiEntitiesHavingLabels(
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
                        row.getColumn(4).getStringArrayList());
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

  private EntityQueryRequest buildServiceQueryRequest(String serviceId) {
    return EntityQueryRequest.newBuilder()
        .setEntityType(EntityType.SERVICE.name())
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getServiceIdColumnName()))
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getServiceNameColumnName()))
        .addSelection(
            buildSelectionExpression(entityQueryServiceConfig.getServiceEnvironmentColumnName()))
        .setFilter(buildServiceIdFilter(serviceId))
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
        .setFilter(buildApiLabelFilter(apiLabelIds))
        .build();
  }

  private Expression buildSelectionExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName).build())
        .build();
  }

  private Filter buildServiceIdFilter(String serviceId) {
    return Filter.newBuilder()
        .setLhs(buildSelectionExpression(entityQueryServiceConfig.getServiceIdColumnName()))
        .setOperator(Operator.EQ)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setValueType(ValueType.STRING)
                                .setString(serviceId))))
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
}
