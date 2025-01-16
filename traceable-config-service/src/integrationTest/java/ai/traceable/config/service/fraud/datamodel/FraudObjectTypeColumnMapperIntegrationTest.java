package ai.traceable.config.service.fraud.datamodel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.service.fraud.ResourceUtils;
import ai.traceable.fraud.datamodel.config.service.FraudDataModelTestUtils;
import ai.traceable.fraud.datamodel.config.service.store.FraudObjectTypesDocumentStore;
import ai.traceable.fraud.datamodel.config.service.v1.ColumnMapping;
import ai.traceable.fraud.datamodel.config.service.v1.ColumnMappingMeta;
import ai.traceable.fraud.datamodel.config.service.v1.EntityFieldMetadata;
import ai.traceable.fraud.datamodel.config.service.v1.EntityType;
import ai.traceable.fraud.datamodel.config.service.v1.EventType;
import ai.traceable.fraud.datamodel.config.service.v1.FieldMetadata;
import ai.traceable.fraud.datamodel.config.service.v1.FieldType;
import ai.traceable.fraud.datamodel.config.service.v1.FraudDataModelConfigServiceGrpc;
import ai.traceable.fraud.datamodel.config.service.v1.GetEntityTypesRequest;
import ai.traceable.fraud.datamodel.config.service.v1.GetRelationshipTypesRequest;
import ai.traceable.fraud.datamodel.config.service.v1.GetTypesRequest;
import ai.traceable.fraud.datamodel.config.service.v1.GetTypesResponse;
import ai.traceable.fraud.datamodel.config.service.v1.MetricDataType;
import ai.traceable.fraud.datamodel.config.service.v1.MetricType;
import ai.traceable.fraud.datamodel.config.service.v1.RelationshipType;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertEntityTypeRequest;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertEntityTypeResponse;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertEventTypeRequest;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertEventTypeResponse;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertMetricTypeRequest;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertMetricTypeResponse;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertRelationshipTypeRequest;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertRelationshipTypeResponse;
import com.typesafe.config.ConfigFactory;
import io.grpc.Deadline;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.grpcutils.client.InProcessGrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FraudObjectTypeColumnMapperIntegrationTest {
  private static FraudDataModelConfigServiceGrpc.FraudDataModelConfigServiceBlockingStub
      fraudDataModelConfigServiceBlockingStub;
  public static final String TENANT_ID = "tenant1";

  private static final Collection Fraud_OBJECT_TYPES_COLLECTION = getFraudObjectTypesStore();
  protected static final String SERVICE_NAME = "traceable-config-service";

  protected static InProcessGrpcChannelRegistry channelRegistry;

  @BeforeAll
  static void init() {
    IntegrationTestServerUtil.startServices(new String[] {SERVICE_NAME});
    channelRegistry = new InProcessGrpcChannelRegistry();
    fraudDataModelConfigServiceBlockingStub =
        FraudDataModelConfigServiceGrpc.newBlockingStub(channelRegistry.forName(SERVICE_NAME))
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @BeforeEach
  public void delete() {
    Fraud_OBJECT_TYPES_COLLECTION.deleteAll();
  }

  @AfterAll
  public static void teardown() {
    channelRegistry.shutdown(Deadline.after(1, TimeUnit.SECONDS));
    IntegrationTestServerUtil.shutdownServices();
  }

  @Test
  public void testUpdateMappings_forEntityType() throws IOException {
    EntityType entityType =
        ResourceUtils.readProto("fraud/datamodel/test_entity_type.json", EntityType.newBuilder())
            .build();
    UpsertEntityTypeRequest request =
        UpsertEntityTypeRequest.newBuilder()
            .setId(entityType.getId())
            .setIdSet(entityType.getIdSet())
            .setLifecycle(entityType.getLifecycle())
            .putAllFieldsMeta(entityType.getFieldsMetaMap())
            .build();
    UpsertEntityTypeResponse response =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> fraudDataModelConfigServiceBlockingStub.upsertEntityType(request));
    Assertions.assertNotNull(response.getEntityType());

    ColumnMappingMeta createdColumnMappings = response.getEntityType().getColumnMappingMeta();

    // upsert the same type again, verify mappings don't change.
    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> fraudDataModelConfigServiceBlockingStub.upsertEntityType(request));
    Assertions.assertNotNull(response.getEntityType());

    ColumnMappingMeta updatedColumnMappings = response.getEntityType().getColumnMappingMeta();

    assertEquals(createdColumnMappings, updatedColumnMappings);

    // upsert the type with a new field, verify previous mappings stay the same.
    entityType =
        entityType.toBuilder()
            .putFieldsMeta(
                "updated_field",
                EntityFieldMetadata.newBuilder().setFieldType(FieldType.FIELD_TYPE_STR).build())
            .build();
    UpsertEntityTypeRequest updateRequest =
        UpsertEntityTypeRequest.newBuilder()
            .setId(entityType.getId())
            .setIdSet(entityType.getIdSet())
            .setLifecycle(entityType.getLifecycle())
            .putAllFieldsMeta(entityType.getFieldsMetaMap())
            .build();
    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> fraudDataModelConfigServiceBlockingStub.upsertEntityType(updateRequest));
    Assertions.assertNotNull(response.getEntityType());
    updatedColumnMappings = response.getEntityType().getColumnMappingMeta();

    Map<String, ColumnMapping> updatedFieldMappings = updatedColumnMappings.getColumnMappingMap();
    for (Map.Entry<String, ColumnMapping> entry :
        createdColumnMappings.getColumnMappingMap().entrySet()) {
      // verify mappings did not change for any of the previous columns
      ColumnMapping updatedColumnMapping = updatedFieldMappings.get(entry.getKey());
      assertEquals(entry.getValue(), updatedColumnMapping);
    }
    // verify that the new field has a mapping
    ColumnMapping newFieldMapping = updatedFieldMappings.get("updated_field");
    Assertions.assertNotNull(newFieldMapping);

    Map<String, String> updatedRevColMappings = updatedColumnMappings.getRevColumnMappingMap();
    for (Map.Entry<String, String> entry :
        createdColumnMappings.getRevColumnMappingMap().entrySet()) {
      // verify rev mappings did not change for any of the previous columns
      String revMapping = updatedRevColMappings.get(entry.getKey());
      assertEquals(entry.getValue(), revMapping);
    }
    // verify that the new field has a rev mapping
    String newFieldRevMapping = updatedRevColMappings.get(newFieldMapping.getColumnId());
    assertEquals("updated_field", newFieldRevMapping);
  }

  @Test
  public void testUpdateMappings_forMetricType() throws IOException {
    MetricType metricType1 =
        ResourceUtils.readProto("fraud/datamodel/test_metric_type_1.json", MetricType.newBuilder())
            .build();

    UpsertMetricTypeRequest request =
        UpsertMetricTypeRequest.newBuilder()
            .setId(metricType1.getId())
            .setMetricDataType(MetricDataType.METRIC_DATA_TYPE_COUNTER)
            .putAllFieldsMeta(metricType1.getFieldsMetaMap())
            .setTimestampField(metricType1.getTimestampField())
            .build();
    UpsertMetricTypeResponse response =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> fraudDataModelConfigServiceBlockingStub.upsertMetricType(request));
    Assertions.assertNotNull(response.getMetricType());
    assertEquals(
        response.getMetricType().getFieldsMetaCount(),
        response.getMetricType().getColumnMappingMeta().getRevColumnMappingCount());
    assertEquals(
        response.getMetricType().getFieldsMetaCount(),
        response.getMetricType().getColumnMappingMeta().getColumnMappingCount());

    ColumnMappingMeta createdColumnMappings = response.getMetricType().getColumnMappingMeta();
    assertTrue(
        createdColumnMappings
            .getColumnMappingMap()
            .get("ip_address")
            .getColumnId()
            .startsWith("idx"));

    // upsert the same type again, verify mappings don't change.
    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> fraudDataModelConfigServiceBlockingStub.upsertMetricType(request));
    Assertions.assertNotNull(response.getMetricType());
    ColumnMappingMeta updatedColumnMappings = response.getMetricType().getColumnMappingMeta();
    assertEquals(createdColumnMappings, updatedColumnMappings);

    // upsert another metric type. for columns of same name (As those in the previous type),
    // we should get the same mappings for this type
    MetricType metricType2 =
        ResourceUtils.readProto("fraud/datamodel/test_metric_type_2.json", MetricType.newBuilder())
            .build();
    UpsertMetricTypeRequest request2 =
        UpsertMetricTypeRequest.newBuilder()
            .setId(metricType2.getId())
            .setMetricDataType(MetricDataType.METRIC_DATA_TYPE_COUNTER)
            .putAllFieldsMeta(metricType2.getFieldsMetaMap())
            .setTimestampField(metricType2.getTimestampField())
            .build();
    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> fraudDataModelConfigServiceBlockingStub.upsertMetricType(request2));
    Assertions.assertNotNull(response.getMetricType());
    assertEquals(
        response.getMetricType().getFieldsMetaCount(),
        response.getMetricType().getColumnMappingMeta().getRevColumnMappingCount());
    assertEquals(
        response.getMetricType().getFieldsMetaCount(),
        response.getMetricType().getColumnMappingMeta().getColumnMappingCount());

    ColumnMappingMeta columnMappingsForMetric1 = createdColumnMappings;
    Assertions.assertEquals(
        "start_time_millis_ts",
        createdColumnMappings.getColumnMappingMap().get("start_time_millis_ts").getColumnId());
    ColumnMappingMeta columnMappingsForMetric2 = response.getMetricType().getColumnMappingMeta();
    // verify that additional fields of metric2 have mappings.
    for (Map.Entry<String, ColumnMapping> entryForMetric2 :
        columnMappingsForMetric2.getColumnMappingMap().entrySet()) {
      if (columnMappingsForMetric1.getColumnMappingMap().containsKey(entryForMetric2.getKey())) {
        assertEquals(
            columnMappingsForMetric1.getColumnMappingMap().get(entryForMetric2.getKey()),
            entryForMetric2.getValue(),
            entryForMetric2.getKey());
      } else {
        Assertions.assertNotNull(entryForMetric2.getValue());
      }
    }

    // upsert metric type1 again
    // upsert the same type again, verify mappings don't change.
    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> fraudDataModelConfigServiceBlockingStub.upsertMetricType(request));
    Assertions.assertNotNull(response.getMetricType());
    updatedColumnMappings = response.getMetricType().getColumnMappingMeta();
    assertEquals(createdColumnMappings, updatedColumnMappings);
    assertEquals(
        response.getMetricType().getFieldsMetaCount(),
        response.getMetricType().getColumnMappingMeta().getRevColumnMappingCount());
    assertEquals(
        response.getMetricType().getFieldsMetaCount(),
        response.getMetricType().getColumnMappingMeta().getColumnMappingCount());
  }

  @Test
  public void testUpdateMappings_forEventType() throws IOException {
    EventType eventType =
        ResourceUtils.readProto("fraud/datamodel/test_event_type.json", EventType.newBuilder())
            .build();
    UpsertEventTypeRequest request =
        UpsertEventTypeRequest.newBuilder()
            .setId(eventType.getId())
            .putAllFieldsMeta(eventType.getFieldsMetaMap())
            .setTimestampField(eventType.getTimestampField())
            .build();
    UpsertEventTypeResponse response =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> fraudDataModelConfigServiceBlockingStub.upsertEventType(request));
    Assertions.assertNotNull(response.getEventType());

    ColumnMappingMeta createdColumnMappings = response.getEventType().getColumnMappingMeta();
    Assertions.assertEquals(
        "start_time_millis_ts",
        createdColumnMappings.getColumnMappingMap().get("start_time_millis_ts").getColumnId());

    // upsert the same type again, verify mappings don't change.
    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> fraudDataModelConfigServiceBlockingStub.upsertEventType(request));
    Assertions.assertNotNull(response.getEventType());

    ColumnMappingMeta updatedColumnMappings = response.getEventType().getColumnMappingMeta();

    assertEquals(createdColumnMappings, updatedColumnMappings);

    // upsert the type with a new field, verify previous mappings stay the same.
    eventType =
        eventType.toBuilder()
            .putFieldsMeta(
                "updated_field",
                FieldMetadata.newBuilder().setFieldType(FieldType.FIELD_TYPE_STR).build())
            .build();
    UpsertEventTypeRequest updateRequest =
        UpsertEventTypeRequest.newBuilder()
            .setId(eventType.getId())
            .putAllFieldsMeta(eventType.getFieldsMetaMap())
            .setTimestampField(eventType.getTimestampField())
            .build();
    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> fraudDataModelConfigServiceBlockingStub.upsertEventType(updateRequest));
    Assertions.assertNotNull(response.getEventType());
    updatedColumnMappings = response.getEventType().getColumnMappingMeta();

    Map<String, ColumnMapping> updatedFieldMappings = updatedColumnMappings.getColumnMappingMap();
    for (Map.Entry<String, ColumnMapping> entry :
        createdColumnMappings.getColumnMappingMap().entrySet()) {
      // verify mappings did not change for any of the previous columns
      ColumnMapping updatedColumnMapping = updatedFieldMappings.get(entry.getKey());
      assertEquals(entry.getValue(), updatedColumnMapping);
    }
    // verify that the new field has a mapping
    ColumnMapping newFieldMapping = updatedFieldMappings.get("updated_field");
    Assertions.assertNotNull(newFieldMapping);

    Map<String, String> updatedRevColMappings = updatedColumnMappings.getRevColumnMappingMap();
    for (Map.Entry<String, String> entry :
        createdColumnMappings.getRevColumnMappingMap().entrySet()) {
      // verify rev mappings did not change for any of the previous columns
      String revMapping = updatedRevColMappings.get(entry.getKey());
      assertEquals(entry.getValue(), revMapping);
    }
    // verify that the new field has a rev mapping
    String newFieldRevMapping = updatedRevColMappings.get(newFieldMapping.getColumnId());
    assertEquals("updated_field", newFieldRevMapping);
  }

  @Test
  public void testUpsertRelationshipTypes() {
    for (RelationshipType relationshipType : FraudDataModelTestUtils.relationshipTypes()) {
      UpsertRelationshipTypeRequest request =
          UpsertRelationshipTypeRequest.newBuilder()
              .setId(relationshipType.getId())
              .setLeftEntityTypeId(relationshipType.getLeftEntityTypeId())
              .setRightEntityTypeId(relationshipType.getRightEntityTypeId())
              .setLeftCardinality(relationshipType.getLeftCardinality())
              .setRightCardinality(relationshipType.getRightCardinality())
              .setRightToLeftNavName(relationshipType.getRightToLeftNavName())
              .setLeftToRightNavName(relationshipType.getLeftToRightNavName())
              .setLifecycle(relationshipType.getLifecycle())
              .putAllFieldsMeta(relationshipType.getFieldsMetaMap())
              .build();
      UpsertRelationshipTypeResponse response =
          RequestContext.forTenantId(TENANT_ID)
              .call(() -> fraudDataModelConfigServiceBlockingStub.upsertRelationshipType(request));
      Assertions.assertNotNull(response.getRelationshipType());
    }
    List<RelationshipType> relationshipTypes =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    fraudDataModelConfigServiceBlockingStub
                        .getRelationshipTypes(GetRelationshipTypesRequest.getDefaultInstance())
                        .getRelationshipTypesList());
    assertEquals(FraudDataModelTestUtils.relationshipTypes().size(), relationshipTypes.size());
    GetTypesResponse allTypes =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    fraudDataModelConfigServiceBlockingStub.getTypes(
                        GetTypesRequest.getDefaultInstance()));
    assertEquals(
        FraudDataModelTestUtils.relationshipTypes().size(), allTypes.getRelationshipTypesCount());
  }

  @Test
  public void testUpsertEntityTypes() {
    for (var entityType : FraudDataModelTestUtils.entityTypes()) {
      UpsertEntityTypeRequest request =
          UpsertEntityTypeRequest.newBuilder()
              .setId(entityType.getId())
              .setIdSet(entityType.getIdSet())
              .setLifecycle(entityType.getLifecycle())
              .putAllFieldsMeta(entityType.getFieldsMetaMap())
              .build();
      var response =
          RequestContext.forTenantId(TENANT_ID)
              .call(() -> fraudDataModelConfigServiceBlockingStub.upsertEntityType(request));
      Assertions.assertNotNull(response.getEntityType());
    }

    var entityTypes =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    fraudDataModelConfigServiceBlockingStub
                        .getEntityTypes(GetEntityTypesRequest.getDefaultInstance())
                        .getEntityTypesList());
    assertEquals(FraudDataModelTestUtils.entityTypes().size(), entityTypes.size());
    var allTypes =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    fraudDataModelConfigServiceBlockingStub.getTypes(
                        GetTypesRequest.getDefaultInstance()));
    assertEquals(FraudDataModelTestUtils.entityTypes().size(), allTypes.getEntityTypesCount());
  }

  private static Collection getFraudObjectTypesStore() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "localhost");
    configMap.put("port", "37017");
    Datastore datastore =
        DatastoreProvider.getDatastore("mongo", ConfigFactory.parseMap(configMap));
    return datastore.getCollection(FraudObjectTypesDocumentStore.FRAUD_OBJECT_TYPES);
  }
}
