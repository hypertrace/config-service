package ai.traceable.blocking.config.service.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.BlockingDataCacheConfig;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EntityFetcherTest {

  private EntityFetcher entityFetcher;
  private EntityQueryServiceConfig entityQueryServiceConfig;
  private EntityQueryServiceClient entityQueryServiceClient;

  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenantId");
  private static final String ENVIRONMENT_ID_COLUMN_NAME = "ENVIRONMENT.id";
  private static final String ENVIRONMENT_NAME_COLUMN_NAME = "ENVIRONMENT.name";

  @BeforeEach
  void setup() {
    entityQueryServiceClient = mock(EntityQueryServiceClient.class);
    entityQueryServiceConfig = mock(EntityQueryServiceConfig.class);
    when(entityQueryServiceConfig.getEnvironmentIdColumnName())
        .thenReturn(ENVIRONMENT_ID_COLUMN_NAME);
    when(entityQueryServiceConfig.getEnvironmentNameColumnName())
        .thenReturn(ENVIRONMENT_NAME_COLUMN_NAME);
    when(entityQueryServiceConfig.getCacheConfig())
        .thenReturn(
            new BlockingDataCacheConfig(
                ConfigFactory.parseMap(
                    Map.of(
                        "cache",
                        Map.of(
                            "maxCacheSize",
                            10,
                            "expireAfterWriteDuration",
                            "10s",
                            "refreshAfterWriteDuration",
                            "5s")))));

    EnvironmentIdCacheLoader cacheLoader =
        new EnvironmentIdCacheLoader(entityQueryServiceClient, entityQueryServiceConfig);
    entityFetcher = new EntityFetcher(entityQueryServiceConfig, cacheLoader);
  }

  @Test
  public void testGetEnvironmentId() throws ExecutionException, InterruptedException {
    String envName = "envName";
    String envId = "envId";

    when(entityQueryServiceClient.execute(any(), any()))
        .thenReturn(List.of(ResultSetChunk.newBuilder().build()).iterator());
    assertTrue(entityFetcher.getEnvironmentId(REQUEST_CONTEXT, envName).isEmpty());

    when(entityQueryServiceClient.execute(any(), any()))
        .thenReturn(List.of(getResultSetChunk("random", envId)).iterator());
    assertTrue(entityFetcher.getEnvironmentId(REQUEST_CONTEXT, envName).isEmpty());

    when(entityQueryServiceClient.execute(any(), any()))
        .thenReturn(List.of(getResultSetChunk(envName, envId)).iterator());
    assertTrue(entityFetcher.getEnvironmentId(REQUEST_CONTEXT, envName).isEmpty());
    TimeUnit.SECONDS.sleep(6);
    assertEquals(envId, entityFetcher.getEnvironmentId(REQUEST_CONTEXT, envName).get());
  }

  private ResultSetChunk getResultSetChunk(String envName, String envId) {
    ResultSetChunk.Builder resultSetChunkBuilder = ResultSetChunk.newBuilder();
    List<String> columnNames = List.of(ENVIRONMENT_NAME_COLUMN_NAME, ENVIRONMENT_ID_COLUMN_NAME);
    List<ColumnMetadata> columnMetadataBuilders =
        columnNames.stream()
            .map(
                (columnName) ->
                    ColumnMetadata.newBuilder()
                        .setColumnName(columnName)
                        .setValueType(ValueType.STRING)
                        .build())
            .collect(Collectors.toUnmodifiableList());
    resultSetChunkBuilder.setResultSetMetadata(
        ResultSetMetadata.newBuilder().addAllColumnMetadata(columnMetadataBuilders));

    resultSetChunkBuilder.addRow(
        Row.newBuilder()
            .addColumn(Value.newBuilder().setString(envName).setValueType(ValueType.STRING))
            .addColumn(Value.newBuilder().setString(envId).setValueType(ValueType.STRING)));
    return resultSetChunkBuilder.build();
  }
}
