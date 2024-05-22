package ai.traceable.config.service.fraud.datamodel;

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
import ai.traceable.fraud.datamodel.config.service.v1.GetTypesRequest;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertEntityTypeRequest;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertEntityTypeResponse;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertEventTypeRequest;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertEventTypeResponse;
import com.typesafe.config.ConfigFactory;
import io.grpc.Deadline;
import java.io.IOException;
import java.util.HashMap;
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

    Assertions.assertEquals(createdColumnMappings, updatedColumnMappings);

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
      Assertions.assertEquals(entry.getValue(), updatedColumnMapping);
    }
    // verify that the new field has a mapping
    ColumnMapping newFieldMapping = updatedFieldMappings.get("updated_field");
    Assertions.assertNotNull(newFieldMapping);

    Map<String, String> updatedRevColMappings = updatedColumnMappings.getRevColumnMappingMap();
    for (Map.Entry<String, String> entry :
        createdColumnMappings.getRevColumnMappingMap().entrySet()) {
      // verify rev mappings did not change for any of the previous columns
      String revMapping = updatedRevColMappings.get(entry.getKey());
      Assertions.assertEquals(entry.getValue(), revMapping);
    }
    // verify that the new field has a rev mapping
    String newFieldRevMapping = updatedRevColMappings.get(newFieldMapping.getColumnId());
    Assertions.assertEquals("updated_field", newFieldRevMapping);
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

    // upsert the same type again, verify mappings don't change.
    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> fraudDataModelConfigServiceBlockingStub.upsertEventType(request));
    Assertions.assertNotNull(response.getEventType());

    ColumnMappingMeta updatedColumnMappings = response.getEventType().getColumnMappingMeta();

    Assertions.assertEquals(createdColumnMappings, updatedColumnMappings);

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
      Assertions.assertEquals(entry.getValue(), updatedColumnMapping);
    }
    // verify that the new field has a mapping
    ColumnMapping newFieldMapping = updatedFieldMappings.get("updated_field");
    Assertions.assertNotNull(newFieldMapping);

    Map<String, String> updatedRevColMappings = updatedColumnMappings.getRevColumnMappingMap();
    for (Map.Entry<String, String> entry :
        createdColumnMappings.getRevColumnMappingMap().entrySet()) {
      // verify rev mappings did not change for any of the previous columns
      String revMapping = updatedRevColMappings.get(entry.getKey());
      Assertions.assertEquals(entry.getValue(), revMapping);
    }
    // verify that the new field has a rev mapping
    String newFieldRevMapping = updatedRevColMappings.get(newFieldMapping.getColumnId());
    Assertions.assertEquals("updated_field", newFieldRevMapping);
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
    Assertions.assertEquals(FraudDataModelTestUtils.entityTypes().size(), entityTypes.size());
    var allTypes =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    fraudDataModelConfigServiceBlockingStub.getTypes(
                        GetTypesRequest.getDefaultInstance()));
    Assertions.assertEquals(
        FraudDataModelTestUtils.entityTypes().size(), allTypes.getEntityTypesCount());
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
