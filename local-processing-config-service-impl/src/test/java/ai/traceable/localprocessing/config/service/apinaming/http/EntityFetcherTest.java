package ai.traceable.localprocessing.config.service.apinaming.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.localprocessing.config.service.client.EntityQueryServiceClient;
import ai.traceable.localprocessing.config.service.config.EntityQueryServiceConfig;
import ai.traceable.localprocessing.config.service.v1.ServiceRequest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

class EntityFetcherTest {

  private EntityFetcher entityFetcher;
  private EntityQueryServiceConfig entityQueryServiceConfig;
  private EntityQueryServiceClient entityQueryServiceClient;

  private static final String SERVICE_ID_COLUMN_NAME = "SERVICE.id";
  private static final String SERVICE_NAME_COLUMN_NAME = "SERVICE.name";
  private static final String SERVICE_ENVIRONMENT_COLUMN_NAME = "SERVICE.environment";

  @BeforeEach
  void setup() {
    Config mockConfig = buildConfig();
    entityQueryServiceClient = mock(EntityQueryServiceClient.class);
    entityQueryServiceConfig = mock(EntityQueryServiceConfig.class);
    buildEntityQueryServiceConfigMocks();
    EntityCacheLoader entityCacheLoader =
        new EntityCacheLoader(entityQueryServiceClient, entityQueryServiceConfig);
    DelegateEntityCacheLoader delegateEntityCacheLoader =
        new DelegateEntityCacheLoader(mockConfig, entityCacheLoader);
    entityFetcher = new EntityFetcher(mockConfig, delegateEntityCacheLoader);
  }

  @Test
  void testServiceIdFetcherCacheMiss() throws ExecutionException, InterruptedException {
    when(entityQueryServiceClient.execute(any(), any()))
        .thenReturn(List.of(ResultSetChunk.newBuilder().build()).iterator());
    Map<ServiceRequest, Optional<String>> serviceIds = serviceIdFetcher();
    assertTrue(serviceIds.isEmpty());

    when(entityQueryServiceClient.execute(any(), any()))
        .thenReturn(List.of(getResultSetChunk("serviceId")).iterator());
    serviceIds = serviceIdFetcher();

    assertTrue(serviceIds.isEmpty());

    TimeUnit.SECONDS.sleep(6);
    serviceIds = serviceIdFetcher();

    assertEquals(buildServiceRequestServiceIdMap("serviceId", "serviceName"), serviceIds);
  }

  @Test
  void testServiceIdFetcherCacheHit() throws ExecutionException, InterruptedException {
    when(entityQueryServiceClient.execute(any(), any()))
        .thenReturn(List.of(getResultSetChunk("serviceId1")).iterator());
    Map<ServiceRequest, Optional<String>> serviceIds = serviceIdFetcher();
    assertEquals(buildServiceRequestServiceIdMap("serviceId1", "serviceName"), serviceIds);

    when(entityQueryServiceClient.execute(any(), any()))
        .thenReturn(List.of(getResultSetChunk("serviceId2")).iterator());

    TimeUnit.SECONDS.sleep(6);
    serviceIds = serviceIdFetcher();

    assertEquals(buildServiceRequestServiceIdMap("serviceId1", "serviceName"), serviceIds);

    TimeUnit.SECONDS.sleep(6);
    serviceIds = serviceIdFetcher();

    assertEquals(buildServiceRequestServiceIdMap("serviceId2", "serviceName"), serviceIds);
  }

  private Map<ServiceRequest, Optional<String>> serviceIdFetcher() throws ExecutionException {
    return entityFetcher.getServiceIds(
        RequestContext.forTenantId("tenantId"),
        List.of(ServiceRequest.newBuilder().setServiceName("serviceName").build()),
        Optional.of("environment"));
  }

  private Map<ServiceRequest, Optional<String>> buildServiceRequestServiceIdMap(
      String serviceId, String serviceName) {
    return Map.of(
        ServiceRequest.newBuilder().setServiceName(serviceName).build(), Optional.of(serviceId));
  }

  private void buildEntityQueryServiceConfigMocks() {
    when(entityQueryServiceConfig.getServiceIdColumnName()).thenReturn(SERVICE_ID_COLUMN_NAME);
    when(entityQueryServiceConfig.getServiceNameColumnName()).thenReturn(SERVICE_NAME_COLUMN_NAME);
    when(entityQueryServiceConfig.getServiceEnvironmentColumnName())
        .thenReturn(SERVICE_ENVIRONMENT_COLUMN_NAME);
  }

  private ResultSetChunk getResultSetChunk(String serviceId) {
    ResultSetChunk.Builder resultSetChunkBuilder = ResultSetChunk.newBuilder();
    List<String> columnNames =
        List.of(SERVICE_ID_COLUMN_NAME, SERVICE_NAME_COLUMN_NAME, SERVICE_ENVIRONMENT_COLUMN_NAME);
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
            .addColumn(Value.newBuilder().setString(serviceId).setValueType(ValueType.STRING))
            .addColumn(Value.newBuilder().setString("serviceName").setValueType(ValueType.STRING))
            .addColumn(Value.newBuilder().setString("environment").setValueType(ValueType.STRING)));

    return resultSetChunkBuilder.build();
  }

  private Config buildConfig() {
    return ConfigFactory.parseMap(
        Map.of(
            "api.naming.config.entity.fetcher.cache.delegate.refreshAfterWriteDuration", "5s",
            "api.naming.config.entity.fetcher.cache.delegate.expireAfterWriteDuration", "10s",
            "api.naming.config.entity.fetcher.cache.refreshAfterWriteDuration", "10s",
            "api.naming.config.entity.fetcher.cache.expireAfterWriteDuration", "20s"));
  }
}
