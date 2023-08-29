package ai.traceable.entity.fetcher.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import ai.traceable.entity.fetcher.cache.config.EntityQueryServiceConfig;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc.EntityQueryServiceBlockingStub;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EntityQueryServiceClientTest {
  private static final String TENANT_ID = "tenantId";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private EntityQueryServiceClient entityQueryServiceClient;
  private EntityQueryServiceBlockingStub queryServiceBlockingStub;

  @BeforeEach
  void setUp() {
    EntityQueryServiceConfig entityQueryServiceConfig = mock(EntityQueryServiceConfig.class);
    when(entityQueryServiceConfig.getServiceIdColumnName()).thenReturn("ID");
    when(entityQueryServiceConfig.getServiceNameColumnName()).thenReturn("Name");
    when(entityQueryServiceConfig.getServiceEnvironmentColumnName()).thenReturn("Environment");
    when(entityQueryServiceConfig.getTimeout()).thenReturn(Duration.of(10, ChronoUnit.SECONDS));

    queryServiceBlockingStub = mock(EntityQueryServiceBlockingStub.class, RETURNS_DEEP_STUBS);
    entityQueryServiceClient =
        new EntityQueryServiceClient(entityQueryServiceConfig, queryServiceBlockingStub);
  }

  @Test
  void testClient() {
    when(queryServiceBlockingStub
            .withDeadlineAfter(10000L, TimeUnit.MILLISECONDS)
            .execute(buildRequest()))
        .thenReturn(List.of(getResultSetChunk()).iterator());

    Optional<ServiceIdentifierEntity> serviceIdentifierEntity =
        entityQueryServiceClient.getServiceEntity(
            REQUEST_CONTEXT.buildInternalContextualKey("serviceId"));
    assertEquals(
        Optional.of(new ServiceIdentifierEntity("serviceName", Optional.of("environment"))),
        serviceIdentifierEntity);

    serviceIdentifierEntity =
        entityQueryServiceClient.getServiceEntity(
            REQUEST_CONTEXT.buildInternalContextualKey("id2"));
    assertTrue(serviceIdentifierEntity.isEmpty());
  }

  private static EntityQueryRequest buildRequest() {
    return EntityQueryRequest.newBuilder()
        .setEntityType(EntityType.SERVICE.name())
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("ID")))
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Name")))
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Environment")))
        .setFilter(
            Filter.newBuilder()
                .setLhs(
                    Expression.newBuilder()
                        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("ID")))
                .setOperator(Operator.EQ)
                .setRhs(
                    Expression.newBuilder()
                        .setLiteral(
                            LiteralConstant.newBuilder()
                                .setValue(
                                    Value.newBuilder()
                                        .setValueType(ValueType.STRING)
                                        .setString("serviceId")))))
        .build();
  }

  private ResultSetChunk getResultSetChunk() {
    ResultSetChunk.Builder resultSetChunkBuilder = ResultSetChunk.newBuilder();
    List<String> columnNames = List.of("ID", "Name", "Environment");
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
            .addColumn(Value.newBuilder().setString("serviceId").setValueType(ValueType.STRING))
            .addColumn(Value.newBuilder().setString("serviceName").setValueType(ValueType.STRING))
            .addColumn(Value.newBuilder().setString("environment").setValueType(ValueType.STRING)));

    return resultSetChunkBuilder.build();
  }
}
