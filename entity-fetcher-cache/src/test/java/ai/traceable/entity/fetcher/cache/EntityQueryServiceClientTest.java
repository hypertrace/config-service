package ai.traceable.entity.fetcher.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import ai.traceable.entity.fetcher.cache.config.EntityQueryServiceConfig;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.grpcutils.context.ContextualKey;
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
    when(entityQueryServiceConfig.getApiIdColumnName()).thenReturn("apiId");
    when(entityQueryServiceConfig.getApiNameColumnName()).thenReturn("apiName");
    when(entityQueryServiceConfig.getApiUrlPatternColumnName()).thenReturn("apiUrlPattern");
    when(entityQueryServiceConfig.getApiTypeColumnName()).thenReturn("apiType");
    when(entityQueryServiceConfig.getHttpMethodColumnName()).thenReturn("httpMethod");
    when(entityQueryServiceConfig.getApiResolvedUrlPatternsColumnName())
        .thenReturn("apiResolvedUrlPatterns");
    when(entityQueryServiceConfig.getApiServiceNameColumnName()).thenReturn("apiServiceName");
    when(entityQueryServiceConfig.getApiEnvironmentColumnName()).thenReturn("apiEnvironment");
    when(entityQueryServiceConfig.getApiIsLearntStatusColumnName()).thenReturn("apiIsLearnt");
    when(entityQueryServiceConfig.getApiLabelsColumnName()).thenReturn("apiLabels");
    when(entityQueryServiceConfig.getTimeout()).thenReturn(Duration.of(10, ChronoUnit.SECONDS));

    queryServiceBlockingStub = mock(EntityQueryServiceBlockingStub.class, RETURNS_DEEP_STUBS);
    entityQueryServiceClient =
        new EntityQueryServiceClient(entityQueryServiceConfig, queryServiceBlockingStub);
  }

  @Test
  void testClient() {
    when(queryServiceBlockingStub
            .withDeadlineAfter(10000L, TimeUnit.MILLISECONDS)
            .execute(buildServiceIdsRequest()))
        .thenReturn(List.of(getServiceIdsResultSetChunk()).iterator());

    ContextualKey<String> contextualKey1 = REQUEST_CONTEXT.buildInternalContextualKey("serviceId1");
    ContextualKey<String> contextualKey2 = REQUEST_CONTEXT.buildInternalContextualKey("serviceId2");
    Map<ContextualKey<String>, Optional<ServiceIdentifierEntity>> serviceIdentifierEntities =
        entityQueryServiceClient.getServiceEntities(Set.of(contextualKey1, contextualKey2));
    assertEquals(2, serviceIdentifierEntities.size());
    assertEquals(
        Optional.of(new ServiceIdentifierEntity("serviceName1", Optional.of("environment1"))),
        serviceIdentifierEntities.get(contextualKey1));
    assertEquals(
        Optional.of(new ServiceIdentifierEntity("serviceName2", Optional.of("environment2"))),
        serviceIdentifierEntities.get(contextualKey2));

    contextualKey1 = REQUEST_CONTEXT.buildInternalContextualKey("id2");
    serviceIdentifierEntities = entityQueryServiceClient.getServiceEntities(Set.of(contextualKey1));
    assertEquals(1, serviceIdentifierEntities.size());
    assertTrue(serviceIdentifierEntities.get(contextualKey1).isEmpty());
  }

  @Test
  void testClient_apis() {
    when(queryServiceBlockingStub
            .withDeadlineAfter(10000L, TimeUnit.MILLISECONDS)
            .execute(buildApiIdsRequest()))
        .thenReturn(List.of(getApiIdsResultSetChunk()).iterator());
    ContextualKey<String> apiId1Key = REQUEST_CONTEXT.buildInternalContextualKey("apiId1");
    ContextualKey<String> apiId2Key = REQUEST_CONTEXT.buildInternalContextualKey("apiId2");
    ContextualKey<String> apiId3Key = REQUEST_CONTEXT.buildInternalContextualKey("apiId3");
    Map<ContextualKey<String>, Optional<ApiIdentifierEntity>> apiIdentifierEntitiesMap =
        entityQueryServiceClient.getApiEntities(Set.of(apiId1Key, apiId2Key, apiId3Key));

    assertEquals(3, apiIdentifierEntitiesMap.size());
    assertEquals(
        Optional.of(
            new ApiIdentifierEntity(
                "apiId1", "apiName1", "/api1", List.of("/api1"), Collections.emptyList())),
        apiIdentifierEntitiesMap.get(apiId1Key));
    assertEquals(
        Optional.of(
            new ApiIdentifierEntity(
                "apiId2", "apiName2", "/api2", List.of("/api2"), Collections.emptyList())),
        apiIdentifierEntitiesMap.get(apiId2Key));
    assertEquals(Optional.empty(), apiIdentifierEntitiesMap.get(apiId3Key));
  }

  @Test
  void testClient_apis_havingLabels() {
    when(queryServiceBlockingStub
            .withDeadlineAfter(10000L, TimeUnit.MILLISECONDS)
            .execute(buildApiLabelIdsRequest()))
        .thenReturn(List.of(getApiLabelIdsResultSetChunk()).iterator());
    ContextualKey<String> labelId1Key = REQUEST_CONTEXT.buildInternalContextualKey("labelId1");
    ContextualKey<String> labelId2Key = REQUEST_CONTEXT.buildInternalContextualKey("labelId2");
    ContextualKey<String> labelId3Key = REQUEST_CONTEXT.buildInternalContextualKey("labelId3");
    Map<ContextualKey<String>, Set<ApiIdentifierEntity>> apiIdentifierEntitiesMap =
        entityQueryServiceClient.getApiEntitiesHavingLabels(
            Set.of(labelId1Key, labelId2Key, labelId3Key));

    assertEquals(3, apiIdentifierEntitiesMap.size());
    assertEquals(
        Set.of(
            new ApiIdentifierEntity(
                "apiId1", "apiName1", "/api1", List.of("/api1"), List.of("labelId1", "labelId2")),
            new ApiIdentifierEntity(
                "apiId2", "apiName2", "/api2", List.of("/api2"), List.of("labelId1"))),
        apiIdentifierEntitiesMap.get(labelId1Key));
    assertEquals(
        Set.of(
            new ApiIdentifierEntity(
                "apiId1", "apiName1", "/api1", List.of("/api1"), List.of("labelId1", "labelId2"))),
        apiIdentifierEntitiesMap.get(labelId2Key));
    assertEquals(Set.of(), apiIdentifierEntitiesMap.get(labelId3Key));
  }

  private static EntityQueryRequest buildServiceIdsRequest() {
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
                .setOperator(Operator.IN)
                .setRhs(
                    Expression.newBuilder()
                        .setLiteral(
                            LiteralConstant.newBuilder()
                                .setValue(
                                    Value.newBuilder()
                                        .setValueType(ValueType.STRING_ARRAY)
                                        .addAllStringArray(List.of("serviceId1", "serviceId2"))))))
        .build();
  }

  private ResultSetChunk getServiceIdsResultSetChunk() {
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
            .addColumn(Value.newBuilder().setString("serviceId1").setValueType(ValueType.STRING))
            .addColumn(Value.newBuilder().setString("serviceName1").setValueType(ValueType.STRING))
            .addColumn(
                Value.newBuilder().setString("environment1").setValueType(ValueType.STRING)));
    resultSetChunkBuilder.addRow(
        Row.newBuilder()
            .addColumn(Value.newBuilder().setString("serviceId2").setValueType(ValueType.STRING))
            .addColumn(Value.newBuilder().setString("serviceName2").setValueType(ValueType.STRING))
            .addColumn(
                Value.newBuilder().setString("environment2").setValueType(ValueType.STRING)));
    return resultSetChunkBuilder.build();
  }

  private static EntityQueryRequest buildApiIdsRequest() {
    return EntityQueryRequest.newBuilder()
        .setEntityType(EntityType.API.name())
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiId")))
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiName")))
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiUrlPattern")))
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(
                    ColumnIdentifier.newBuilder().setColumnName("apiResolvedUrlPatterns")))
        .setFilter(
            Filter.newBuilder()
                .setLhs(
                    Expression.newBuilder()
                        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiId")))
                .setOperator(Operator.IN)
                .setRhs(
                    Expression.newBuilder()
                        .setLiteral(
                            LiteralConstant.newBuilder()
                                .setValue(
                                    Value.newBuilder()
                                        .setValueType(ValueType.STRING_ARRAY)
                                        .addAllStringArray(
                                            List.of("apiId2", "apiId1", "apiId3"))))))
        .build();
  }

  private ResultSetChunk getApiIdsResultSetChunk() {
    ResultSetChunk.Builder resultSetChunkBuilder = ResultSetChunk.newBuilder();
    List<String> columnNames =
        List.of("apiId", "apiName", "apiUrlPattern", "apiResolvedUrlPatterns");
    List<ColumnMetadata> columnMetadataBuilders =
        columnNames.stream()
            .map(
                (columnName) ->
                    ColumnMetadata.newBuilder()
                        .setColumnName(columnName)
                        .setValueType(
                            columnName.endsWith("s") ? ValueType.STRING_ARRAY : ValueType.STRING)
                        .build())
            .collect(Collectors.toUnmodifiableList());
    resultSetChunkBuilder.setResultSetMetadata(
        ResultSetMetadata.newBuilder().addAllColumnMetadata(columnMetadataBuilders));

    resultSetChunkBuilder
        .addRow(
            Row.newBuilder()
                .addColumn(Value.newBuilder().setString("apiId1").setValueType(ValueType.STRING))
                .addColumn(Value.newBuilder().setString("apiName1").setValueType(ValueType.STRING))
                .addColumn(Value.newBuilder().setString("/api1").setValueType(ValueType.STRING))
                .addColumn(
                    Value.newBuilder()
                        .addAllStringArray(List.of("/api1"))
                        .setValueType(ValueType.STRING_ARRAY)))
        .addRow(
            Row.newBuilder()
                .addColumn(Value.newBuilder().setString("apiId2").setValueType(ValueType.STRING))
                .addColumn(Value.newBuilder().setString("apiName2").setValueType(ValueType.STRING))
                .addColumn(Value.newBuilder().setString("/api2").setValueType(ValueType.STRING))
                .addColumn(
                    Value.newBuilder()
                        .addAllStringArray(List.of("/api2"))
                        .setValueType(ValueType.STRING_ARRAY)));

    return resultSetChunkBuilder.build();
  }

  private static EntityQueryRequest buildApiLabelIdsRequest() {
    return EntityQueryRequest.newBuilder()
        .setEntityType(EntityType.API.name())
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiId")))
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiName")))
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiUrlPattern")))
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(
                    ColumnIdentifier.newBuilder().setColumnName("apiResolvedUrlPatterns")))
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiLabels")))
        .setFilter(
            Filter.newBuilder()
                .setLhs(
                    Expression.newBuilder()
                        .setColumnIdentifier(
                            ColumnIdentifier.newBuilder().setColumnName("apiLabels")))
                .setOperator(Operator.IN)
                .setRhs(
                    Expression.newBuilder()
                        .setLiteral(
                            LiteralConstant.newBuilder()
                                .setValue(
                                    Value.newBuilder()
                                        .setValueType(ValueType.STRING_ARRAY)
                                        .addAllStringArray(
                                            List.of("labelId3", "labelId2", "labelId1"))))))
        .build();
  }

  private ResultSetChunk getApiLabelIdsResultSetChunk() {
    ResultSetChunk.Builder resultSetChunkBuilder = ResultSetChunk.newBuilder();
    List<String> columnNames =
        List.of("apiId", "apiName", "apiUrlPattern", "apiResolvedUrlPatterns", "apiLabels");
    List<ColumnMetadata> columnMetadataBuilders =
        columnNames.stream()
            .map(
                (columnName) ->
                    ColumnMetadata.newBuilder()
                        .setColumnName(columnName)
                        .setValueType(
                            columnName.endsWith("s") ? ValueType.STRING_ARRAY : ValueType.STRING)
                        .build())
            .collect(Collectors.toUnmodifiableList());
    resultSetChunkBuilder.setResultSetMetadata(
        ResultSetMetadata.newBuilder().addAllColumnMetadata(columnMetadataBuilders));

    resultSetChunkBuilder
        .addRow(
            Row.newBuilder()
                .addColumn(Value.newBuilder().setString("apiId1").setValueType(ValueType.STRING))
                .addColumn(Value.newBuilder().setString("apiName1").setValueType(ValueType.STRING))
                .addColumn(Value.newBuilder().setString("/api1").setValueType(ValueType.STRING))
                .addColumn(
                    Value.newBuilder()
                        .addAllStringArray(List.of("/api1"))
                        .setValueType(ValueType.STRING_ARRAY))
                .addColumn(
                    Value.newBuilder()
                        .addAllStringArray(List.of("labelId1", "labelId2"))
                        .setValueType(ValueType.STRING_ARRAY)))
        .addRow(
            Row.newBuilder()
                .addColumn(Value.newBuilder().setString("apiId2").setValueType(ValueType.STRING))
                .addColumn(Value.newBuilder().setString("apiName2").setValueType(ValueType.STRING))
                .addColumn(Value.newBuilder().setString("/api2").setValueType(ValueType.STRING))
                .addColumn(
                    Value.newBuilder()
                        .addAllStringArray(List.of("/api2"))
                        .setValueType(ValueType.STRING_ARRAY))
                .addColumn(
                    Value.newBuilder()
                        .addAllStringArray(List.of("labelId1"))
                        .setValueType(ValueType.STRING_ARRAY)));

    return resultSetChunkBuilder.build();
  }

  @Test
  void testClient_getAllLearntHttpApiEndpoints() {
    String serviceName = "testServiceName";
    String environment = "testEnv";
    when(queryServiceBlockingStub
            .withDeadlineAfter(10000L, TimeUnit.MILLISECONDS)
            .execute(buildLearntApiQueryRequest(serviceName, environment, "HTTP")))
        .thenReturn(List.of(getLearntApiEndpointsResultSetChunk()).iterator());

    Stream<StreamingApiMappingProvider.HttpApiDetails> result =
        entityQueryServiceClient.getAllLearntHttpApiEndpoints(
            REQUEST_CONTEXT, serviceName, environment);

    List<StreamingApiMappingProvider.HttpApiDetails> apiEntities =
        result.collect(Collectors.toList());
    assertEquals(2, apiEntities.size());

    StreamingApiMappingProvider.HttpApiDetails firstApi = apiEntities.get(0);
    assertEquals("learntApiId1", firstApi.getApiId());
    assertEquals(List.of("/learnt/api1", "/learnt/api1/v2"), firstApi.getResolvedUrlPatterns());

    StreamingApiMappingProvider.HttpApiDetails secondApi = apiEntities.get(1);
    assertEquals("learntApiId2", secondApi.getApiId());
    assertEquals(List.of("/learnt/api2"), secondApi.getResolvedUrlPatterns());
  }

  private static EntityQueryRequest buildLearntApiQueryRequest(
      String serviceName, String environment, String apiType) {
    // Match production code's filter structure: only RHS is set with the literal and operator EQ
    Filter serviceNameFilter =
        Filter.newBuilder()
            .setOperator(Operator.EQ)
            .setLhs(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("apiServiceName")))
            .setRhs(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder()
                                    .setValueType(ValueType.STRING)
                                    .setString(serviceName))))
            .build();
    Filter environmentFilter =
        Filter.newBuilder()
            .setOperator(Operator.EQ)
            .setLhs(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("apiEnvironment")))
            .setRhs(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder()
                                    .setValueType(ValueType.STRING)
                                    .setString(environment))))
            .build();
    Filter apiTypeFilter =
        Filter.newBuilder()
            .setOperator(Operator.EQ)
            .setLhs(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiType")))
            .setRhs(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder()
                                    .setValueType(ValueType.STRING)
                                    .setString(apiType))))
            .build();
    Filter isLearntFilter =
        Filter.newBuilder()
            .setLhs(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("apiIsLearnt")))
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
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiId")))
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("httpMethod")))
        .addSelection(
            Expression.newBuilder()
                .setColumnIdentifier(
                    ColumnIdentifier.newBuilder().setColumnName("apiResolvedUrlPatterns")))
        .setFilter(
            Filter.newBuilder()
                .setOperator(Operator.AND)
                .addChildFilter(isLearntFilter)
                .addChildFilter(serviceNameFilter)
                .addChildFilter(environmentFilter)
                .addChildFilter(apiTypeFilter))
        .build();
  }

  private ResultSetChunk getLearntApiEndpointsResultSetChunk() {
    ResultSetChunk.Builder resultSetChunkBuilder = ResultSetChunk.newBuilder();
    List<String> columnNames = List.of("apiId", "httpMethod", "apiResolvedUrlPatterns");
    List<ColumnMetadata> columnMetadataBuilders =
        columnNames.stream()
            .map(
                (columnName) ->
                    ColumnMetadata.newBuilder()
                        .setColumnName(columnName)
                        .setValueType(
                            columnName.endsWith("s") ? ValueType.STRING_ARRAY : ValueType.STRING)
                        .build())
            .collect(Collectors.toUnmodifiableList());
    resultSetChunkBuilder.setResultSetMetadata(
        ResultSetMetadata.newBuilder().addAllColumnMetadata(columnMetadataBuilders));

    resultSetChunkBuilder
        .addRow(
            Row.newBuilder()
                .addColumn(
                    Value.newBuilder().setString("learntApiId1").setValueType(ValueType.STRING))
                .addColumn(Value.newBuilder().setString("method1").setValueType(ValueType.STRING))
                .addColumn(
                    Value.newBuilder()
                        .addAllStringArray(List.of("/learnt/api1", "/learnt/api1/v2"))
                        .setValueType(ValueType.STRING_ARRAY)))
        .addRow(
            Row.newBuilder()
                .addColumn(
                    Value.newBuilder().setString("learntApiId2").setValueType(ValueType.STRING))
                .addColumn(Value.newBuilder().setString("method2").setValueType(ValueType.STRING))
                .addColumn(
                    Value.newBuilder()
                        .addAllStringArray(List.of("/learnt/api2"))
                        .setValueType(ValueType.STRING_ARRAY)))
        // add a bad row (missing data) - this should get filtered out
        .addRow(
            Row.newBuilder()
                .addColumn(
                    Value.newBuilder().setString("learntApiId3").setValueType(ValueType.STRING))
                .addColumn(
                    Value.newBuilder().setString("/learnt/api3").setValueType(ValueType.STRING)));

    return resultSetChunkBuilder.build();
  }
}
