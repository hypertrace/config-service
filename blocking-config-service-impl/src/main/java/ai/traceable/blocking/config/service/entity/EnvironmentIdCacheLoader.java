package ai.traceable.blocking.config.service.entity;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;

/** Class to cache environment name-to-id info for each tenant */
@Slf4j
public class EnvironmentIdCacheLoader
    extends CacheLoader<ContextualKey<Void>, Map<String, String>> {

  private static final String ENVIRONMENT_ENTITY_TYPE = "ENVIRONMENT";

  private final EntityQueryRequest entityQueryRequest;
  private final EntityQueryServiceClient entityQueryServiceClient;
  private final EntityQueryServiceConfig entityQueryServiceConfig;

  @Inject
  public EnvironmentIdCacheLoader(
      EntityQueryServiceClient entityQueryServiceClient,
      EntityQueryServiceConfig entityQueryServiceConfig) {
    this.entityQueryServiceClient = entityQueryServiceClient;
    this.entityQueryServiceConfig = entityQueryServiceConfig;
    this.entityQueryRequest = buildQueryRequest();
  }

  @Override
  public Map<String, String> load(@Nonnull ContextualKey<Void> contextualKey) {
    try {
      Iterator<ResultSetChunk> resultSetChunkIterator =
          contextualKey.callInContext(
              env ->
                  entityQueryServiceClient.execute(contextualKey.getContext(), entityQueryRequest));
      return Streams.stream(resultSetChunkIterator)
          .map(ResultSetChunk::getRowList)
          .flatMap(List::stream)
          .filter(row -> row.getColumnCount() >= 2)
          .collect(
              Collectors.toUnmodifiableMap(
                  row -> row.getColumn(0).getString(), row -> row.getColumn(1).getString()));
    } catch (Exception e) {
      log.error(
          "Could not fetch environment entities for tenant id:{} with exception:{}",
          contextualKey.getContext().getTenantId(),
          e);
      return null;
    }
  }

  private EntityQueryRequest buildQueryRequest() {
    return EntityQueryRequest.newBuilder()
        .setEntityType(ENVIRONMENT_ENTITY_TYPE)
        .addSelection(
            buildColumnExpression(entityQueryServiceConfig.getEnvironmentNameColumnName()))
        .addSelection(buildColumnExpression(entityQueryServiceConfig.getEnvironmentIdColumnName()))
        .build();
  }

  private Expression buildColumnExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName).build())
        .build();
  }
}
