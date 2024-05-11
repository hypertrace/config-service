package ai.traceable.config.service.fraud.datamodel;

import ai.traceable.fraud.datamodel.config.service.FraudDataModelTestUtils;
import ai.traceable.fraud.datamodel.config.service.store.FraudObjectTypesDocumentStore;
import ai.traceable.fraud.datamodel.config.service.v1.FraudDataModelConfigServiceGrpc;
import ai.traceable.fraud.datamodel.config.service.v1.GetEntityTypesRequest;
import ai.traceable.fraud.datamodel.config.service.v1.GetTypesRequest;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertEntityTypeRequest;
import com.typesafe.config.ConfigFactory;
import io.grpc.Deadline;
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

public class FraudDataModelConfigServiceIntegrationTest {
  private static FraudDataModelConfigServiceGrpc.FraudDataModelConfigServiceBlockingStub
      fraudDataModelConfigServiceBlockingStub;
  public static final String TENANT_ID = "tenant1";

  private static final Collection FRAUD_OBJECT_TYPES_STORE = getFraudObjectTypesStore();
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
    FRAUD_OBJECT_TYPES_STORE.deleteAll();
  }

  @AfterAll
  public static void teardown() {
    channelRegistry.shutdown(Deadline.after(1, TimeUnit.SECONDS));
    IntegrationTestServerUtil.shutdownServices();
  }

  @Test
  public void testUpsertEntityType_VerifyMappings() {
    var entityType = FraudDataModelTestUtils.entityType("test_entity_type", 5);
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
    Assertions.assertNotNull(response.getEntityType().getColumnMappingMeta());
    Assertions.assertEquals(
        entityType.getFieldsMetaCount(), response.getEntityType().getFieldsMetaCount());
    Assertions.assertEquals(
        response.getEntityType().getFieldsMetaCount(),
        response.getEntityType().getColumnMappingMeta().getColumnMappingCount());
    Assertions.assertEquals(
        response.getEntityType().getFieldsMetaCount(),
        response.getEntityType().getColumnMappingMeta().getRevColumnMappingCount());
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
