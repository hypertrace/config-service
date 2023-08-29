package ai.traceable.entity.fetcher.cache;

import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import ai.traceable.entity.fetcher.cache.config.EntityQueryServiceConfig;
import com.google.inject.Inject;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import org.hypertrace.core.grpcutils.context.ContextualKey;
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
        buildQueryRequest(serviceIdContextualKey.getData());
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

  private EntityQueryRequest buildQueryRequest(String serviceId) {
    return EntityQueryRequest.newBuilder()
        .setEntityType(EntityType.SERVICE.name())
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getServiceIdColumnName()))
        .addSelection(buildSelectionExpression(entityQueryServiceConfig.getServiceNameColumnName()))
        .addSelection(
            buildSelectionExpression(entityQueryServiceConfig.getServiceEnvironmentColumnName()))
        .setFilter(buildServiceIdFilter(serviceId))
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
}
