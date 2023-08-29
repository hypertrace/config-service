package ai.traceable.localprocessing.config.service.apinaming.http;

import ai.traceable.entity.fetcher.cache.config.EntityQueryServiceConfig;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.ServiceIdentifier;
import ai.traceable.localprocessing.config.service.client.EntityQueryServiceClient;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
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
public class EntityCacheLoader
    extends CacheLoader<ContextualKey<ServiceIdentifier>, Optional<String>> {

  private final EntityQueryServiceClient entityQueryServiceClient;
  private final EntityQueryServiceConfig entityQueryServiceConfig;

  @Inject
  public EntityCacheLoader(
      EntityQueryServiceClient entityQueryServiceClient,
      EntityQueryServiceConfig entityQueryServiceConfig) {
    this.entityQueryServiceClient = entityQueryServiceClient;
    this.entityQueryServiceConfig = entityQueryServiceConfig;
  }

  @Override
  public Optional<String> load(
      @Nonnull ContextualKey<ServiceIdentifier> serviceIdentifierContextualKey) {
    try {
      EntityQueryRequest entityQueryRequest =
          buildQueryRequest(List.of(serviceIdentifierContextualKey.getData()));
      return getServiceIds(
              entityQueryRequest,
              serviceIdentifierContextualKey,
              List.of(serviceIdentifierContextualKey))
          .get(serviceIdentifierContextualKey);
    } catch (Exception e) {
      ServiceIdentifier serviceIdentifier = serviceIdentifierContextualKey.getData();
      log.error(
          "Could not fetch entity for tenant id:{}, service name: {} and environment : {}",
          serviceIdentifierContextualKey.getContext().getTenantId(),
          serviceIdentifier.getServiceName(),
          serviceIdentifier.getEnvironment(),
          e);
      return Optional.empty();
    }
  }

  @Override
  public Map<ContextualKey<ServiceIdentifier>, Optional<String>> loadAll(
      @Nonnull Iterable<? extends ContextualKey<ServiceIdentifier>> keys) {
    List<ServiceIdentifier> serviceIdentifiers =
        Streams.stream(keys).map(ContextualKey::getData).collect(Collectors.toUnmodifiableList());
    List<ContextualKey<ServiceIdentifier>> contextualServiceIdentifiers =
        Streams.stream(keys).collect(Collectors.toUnmodifiableList());
    EntityQueryRequest entityQueryRequest = buildQueryRequest(serviceIdentifiers);
    ContextualKey<ServiceIdentifier> serviceIdentifierContextualKey = keys.iterator().next();
    try {
      return getServiceIds(
          entityQueryRequest, serviceIdentifierContextualKey, contextualServiceIdentifiers);
    } catch (Exception e) {
      log.error(
          "Could not fetch entity for tenant id:{}, contextual service identifiers: {} with exception:{}",
          serviceIdentifierContextualKey.getContext().getTenantId(),
          contextualServiceIdentifiers,
          e);
      Map<ContextualKey<ServiceIdentifier>, Optional<String>> serviceIdMap = new HashMap<>();
      for (ContextualKey<ServiceIdentifier> key : contextualServiceIdentifiers) {
        serviceIdMap.put(key, Optional.empty());
      }
      return serviceIdMap;
    }
  }

  private Map<ContextualKey<ServiceIdentifier>, Optional<String>> getServiceIds(
      EntityQueryRequest entityQueryRequest,
      ContextualKey<ServiceIdentifier> serviceIdentifierContextualKey,
      List<ContextualKey<ServiceIdentifier>> contextualServiceIdentifiers) {
    Iterator<ResultSetChunk> resultSetChunkIterator =
        serviceIdentifierContextualKey.callInContext(
            serviceIdentifier ->
                entityQueryServiceClient.execute(
                    serviceIdentifierContextualKey.getContext(), entityQueryRequest));

    Map<ContextualKey<ServiceIdentifier>, Optional<String>> serviceIdMap = new HashMap<>();
    for (ContextualKey<ServiceIdentifier> key : contextualServiceIdentifiers) {
      serviceIdMap.put(key, Optional.empty());
    }
    RequestContext requestContext = serviceIdentifierContextualKey.getContext();

    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      for (Row row : chunk.getRowList()) {
        ContextualKey<ServiceIdentifier> key =
            requestContext.buildInternalContextualKey(
                new ServiceIdentifier(
                    row.getColumn(1).getString(),
                    row.getColumn(2).getString().isEmpty()
                        ? Optional.empty()
                        : Optional.of(row.getColumn(2).getString())));
        if (serviceIdMap.containsKey(key)) {
          serviceIdMap.put(key, Optional.of(row.getColumn(0).getString()));
        }
      }
    }
    return serviceIdMap;
  }

  private EntityQueryRequest buildQueryRequest(List<ServiceIdentifier> serviceIdentifiers) {
    EntityQueryRequest.Builder queryRequestBuilder =
        EntityQueryRequest.newBuilder().setEntityType(EntityType.SERVICE.name());

    Filter serviceFilter =
        Filter.newBuilder()
            .setOperator(Operator.OR)
            .addAllChildFilter(
                serviceIdentifiers.stream()
                    .map(
                        serviceIdentifier ->
                            buildFilter(
                                serviceIdentifier.getServiceName(),
                                serviceIdentifier.getEnvironment()))
                    .collect(Collectors.toUnmodifiableList()))
            .build();

    queryRequestBuilder.setFilter(serviceFilter);

    queryRequestBuilder
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getServiceIdColumnName()))
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getServiceNameColumnName()))
        .addSelection(
            buildSelectionExpression(entityQueryServiceConfig.getServiceEnvironmentColumnName()))
        .build();
    return queryRequestBuilder.build();
  }

  private Filter buildFilter(String serviceName, Optional<String> environmentMaybe) {
    Filter.Builder filterBuilder =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(
                Filter.newBuilder()
                    .setLhs(
                        buildSelectionExpression(
                            entityQueryServiceConfig.getServiceNameColumnName()))
                    .setOperator(Operator.EQ)
                    .setRhs(
                        Expression.newBuilder()
                            .setLiteral(
                                LiteralConstant.newBuilder()
                                    .setValue(
                                        Value.newBuilder()
                                            .setValueType(ValueType.STRING)
                                            .setString(serviceName)
                                            .build())
                                    .build()))
                    .build());
    if (environmentMaybe.isEmpty()) {
      return filterBuilder.build();
    }
    return filterBuilder
        .addChildFilter(
            Filter.newBuilder()
                .setLhs(
                    buildSelectionExpression(
                        entityQueryServiceConfig.getServiceEnvironmentColumnName()))
                .setOperator(Operator.EQ)
                .setRhs(
                    Expression.newBuilder()
                        .setLiteral(
                            LiteralConstant.newBuilder()
                                .setValue(
                                    Value.newBuilder()
                                        .setValueType(ValueType.STRING)
                                        .setString(environmentMaybe.get())
                                        .build())
                                .build()))
                .build())
        .build();
  }

  private Expression buildSelectionExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName).build())
        .build();
  }
}
