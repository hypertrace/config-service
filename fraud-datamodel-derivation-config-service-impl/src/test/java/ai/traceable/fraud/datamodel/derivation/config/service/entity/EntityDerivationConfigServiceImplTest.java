package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityCategory;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigFilter;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigServiceGrpc;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigSummariesRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigSummariesResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanProjection;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import io.grpc.StatusRuntimeException;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class EntityDerivationConfigServiceImplTest {

  private EntityDerivationConfigServiceGrpc.EntityDerivationConfigServiceBlockingStub serviceStub;
  private MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void beforeEach() {
    UuidGenerator uuidGenerator = new UuidGenerator();

    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());

    ConfigChangeEventGenerator eventGenerator = Mockito.mock(ConfigChangeEventGenerator.class);
    EntityDerivationConfigStore store =
        new EntityDerivationConfigStore(genericStub, eventGenerator);
    EntityDerivationConfigRequestValidator validator = new EntityDerivationConfigRequestValidator();
    EntityDerivationConfigStoreManager storeManager =
        new EntityDerivationConfigStoreManager(store, uuidGenerator);

    EntityDerivationConfigServiceImpl service =
        new EntityDerivationConfigServiceImpl(storeManager, validator);

    this.mockGenericConfigService.addService(service).start();

    this.serviceStub =
        EntityDerivationConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void testCreateEntityDerivationConfig_Success() {
    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder()
            .setData(createValidConfigData("User Email"))
            .build();

    CreateEntityDerivationConfigResponse response =
        RequestContext.forTenantId("test-tenant")
            .call(() -> serviceStub.createEntityDerivationConfig(request));

    assertNotNull(response);
    assertFalse(response.getEntityDerivationConfig().getId().isEmpty());
    assertEquals("user_email", response.getEntityDerivationConfig().getColumnName());
  }

  @Test
  void testCreateEntityDerivationConfig_ValidationFailure() {
    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder().build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                RequestContext.forTenantId("test-tenant")
                    .call(() -> serviceStub.createEntityDerivationConfig(request)));
    assertTrue(exception.getMessage().contains("Entity derivation configuration data is required"));
  }

  @Test
  void testUpdateEntityDerivationConfig_Success() {
    CreateEntityDerivationConfigResponse createResponse =
        RequestContext.forTenantId("test-tenant")
            .call(
                () ->
                    serviceStub.createEntityDerivationConfig(
                        CreateEntityDerivationConfigRequest.newBuilder()
                            .setData(createValidConfigData("Original Name"))
                            .build()));

    String configId = createResponse.getEntityDerivationConfig().getId();

    UpdateEntityDerivationConfigRequest updateRequest =
        UpdateEntityDerivationConfigRequest.newBuilder()
            .setId(configId)
            .setData(createValidConfigData("Updated Name"))
            .build();

    UpdateEntityDerivationConfigResponse updateResponse =
        RequestContext.forTenantId("test-tenant")
            .call(() -> serviceStub.updateEntityDerivationConfig(updateRequest));

    assertEquals("updated_name", updateResponse.getEntityDerivationConfig().getColumnName());
  }

  @Test
  void testUpdateEntityDerivationConfig_NotFound() {
    UpdateEntityDerivationConfigRequest request =
        UpdateEntityDerivationConfigRequest.newBuilder()
            .setId("non-existent-id")
            .setData(createValidConfigData("Test"))
            .build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                RequestContext.forTenantId("test-tenant")
                    .call(() -> serviceStub.updateEntityDerivationConfig(request)));
    assertTrue(exception.getMessage().contains("No entity derivation config found"));
  }

  @Test
  void testGetEntityDerivationConfigSummaries_Success() {
    RequestContext.forTenantId("test-tenant")
        .call(
            () ->
                serviceStub.createEntityDerivationConfig(
                    CreateEntityDerivationConfigRequest.newBuilder()
                        .setData(createValidConfigData("Entity One"))
                        .build()));

    GetEntityDerivationConfigSummariesRequest request =
        GetEntityDerivationConfigSummariesRequest.newBuilder()
            .setFilter(EntityDerivationConfigFilter.newBuilder().build())
            .build();

    GetEntityDerivationConfigSummariesResponse response =
        RequestContext.forTenantId("test-tenant")
            .call(() -> serviceStub.getEntityDerivationConfigSummaries(request));

    assertEquals(1, response.getSummariesCount());
  }

  @Test
  void testGetEntityDerivationConfigs_Success() {
    RequestContext.forTenantId("test-tenant")
        .call(
            () ->
                serviceStub.createEntityDerivationConfig(
                    CreateEntityDerivationConfigRequest.newBuilder()
                        .setData(createValidConfigData("Test Entity"))
                        .build()));

    GetEntityDerivationConfigsRequest request =
        GetEntityDerivationConfigsRequest.newBuilder()
            .setFilter(EntityDerivationConfigFilter.newBuilder().build())
            .build();

    GetEntityDerivationConfigsResponse response =
        RequestContext.forTenantId("test-tenant")
            .call(() -> serviceStub.getEntityDerivationConfigs(request));

    assertEquals(1, response.getEntityDerivationConfigsCount());
  }

  @Test
  void testDeleteEntityDerivationConfig_Success() {
    CreateEntityDerivationConfigResponse createResponse =
        RequestContext.forTenantId("test-tenant")
            .call(
                () ->
                    serviceStub.createEntityDerivationConfig(
                        CreateEntityDerivationConfigRequest.newBuilder()
                            .setData(createValidConfigData("Test Entity"))
                            .build()));

    String configId = createResponse.getEntityDerivationConfig().getId();

    DeleteEntityDerivationConfigRequest deleteRequest =
        DeleteEntityDerivationConfigRequest.newBuilder()
            .setEntityDerivationConfigId(configId)
            .build();

    RequestContext.forTenantId("test-tenant")
        .call(() -> serviceStub.deleteEntityDerivationConfig(deleteRequest));

    UpdateEntityDerivationConfigRequest updateRequest =
        UpdateEntityDerivationConfigRequest.newBuilder()
            .setId(configId)
            .setData(createValidConfigData("Updated"))
            .build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                RequestContext.forTenantId("test-tenant")
                    .call(() -> serviceStub.updateEntityDerivationConfig(updateRequest)));
    assertTrue(exception.getMessage().contains("No entity derivation config found"));
  }

  private EntityDerivationConfigData createValidConfigData(String displayName) {
    return EntityDerivationConfigData.newBuilder()
        .setDisplayName(displayName)
        .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
        .setEventKind(ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
        .setSpanProjection(
            SpanProjection.newBuilder()
                .addEventDerivationConfigs(EventDerivationConfigDetails.newBuilder()))
        .build();
  }
}
