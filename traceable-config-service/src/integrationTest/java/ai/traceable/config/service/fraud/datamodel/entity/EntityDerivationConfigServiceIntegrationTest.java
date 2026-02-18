package ai.traceable.config.service.fraud.datamodel.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityCategory;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class EntityDerivationConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static EntityDerivationConfigServiceGrpc.EntityDerivationConfigServiceBlockingStub
      serviceStub;

  @BeforeAll
  static void init() {
    serviceStub =
        EntityDerivationConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testCRUD() {
    // Create
    CreateEntityDerivationConfigRequest createRequest =
        CreateEntityDerivationConfigRequest.newBuilder()
            .setData(createValidConfigData("User Email"))
            .build();

    CreateEntityDerivationConfigResponse createResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> serviceStub.createEntityDerivationConfig(createRequest));

    assertTrue(createResponse.hasEntityDerivationConfig());
    EntityDerivationConfig config = createResponse.getEntityDerivationConfig();
    assertFalse(StringUtils.isBlank(config.getId()));
    assertEquals("user_email", config.getColumnName());
    assertEquals("User Email", config.getData().getDisplayName());

    // Update
    UpdateEntityDerivationConfigRequest updateRequest =
        UpdateEntityDerivationConfigRequest.newBuilder()
            .setId(config.getId())
            .setData(createValidConfigData("Updated Email"))
            .build();

    UpdateEntityDerivationConfigResponse updateResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> serviceStub.updateEntityDerivationConfig(updateRequest));

    assertEquals("updated_email", updateResponse.getEntityDerivationConfig().getColumnName());
    assertEquals(
        "Updated Email", updateResponse.getEntityDerivationConfig().getData().getDisplayName());

    // Get Summaries
    GetEntityDerivationConfigSummariesRequest summariesRequest =
        GetEntityDerivationConfigSummariesRequest.newBuilder()
            .setFilter(EntityDerivationConfigFilter.newBuilder().build())
            .build();

    GetEntityDerivationConfigSummariesResponse summariesResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> serviceStub.getEntityDerivationConfigSummaries(summariesRequest));

    assertEquals(1, summariesResponse.getSummariesCount());
    assertEquals(config.getId(), summariesResponse.getSummaries(0).getId());
    assertEquals("Updated Email", summariesResponse.getSummaries(0).getDisplayName());
    assertEquals(
        "system_event_kind_string", summariesResponse.getSummaries(0).getEventKind().getKindId());

    // Get Configs
    GetEntityDerivationConfigsRequest getRequest =
        GetEntityDerivationConfigsRequest.newBuilder()
            .setFilter(EntityDerivationConfigFilter.newBuilder().build())
            .build();

    GetEntityDerivationConfigsResponse getResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> serviceStub.getEntityDerivationConfigs(getRequest));

    assertEquals(1, getResponse.getEntityDerivationConfigsCount());
    assertEquals(config.getId(), getResponse.getEntityDerivationConfigs(0).getId());

    // Get by ID
    GetEntityDerivationConfigsRequest getByIdRequest =
        GetEntityDerivationConfigsRequest.newBuilder()
            .setFilter(EntityDerivationConfigFilter.newBuilder().build())
            .addIds(config.getId())
            .build();

    GetEntityDerivationConfigsResponse getByIdResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> serviceStub.getEntityDerivationConfigs(getByIdRequest));

    assertEquals(1, getByIdResponse.getEntityDerivationConfigsCount());

    // Delete
    DeleteEntityDerivationConfigRequest deleteRequest =
        DeleteEntityDerivationConfigRequest.newBuilder()
            .setEntityDerivationConfigId(config.getId())
            .build();

    RequestContext.forTenantId(TENANT_ID)
        .call(() -> serviceStub.deleteEntityDerivationConfig(deleteRequest));

    // Verify deleted
    GetEntityDerivationConfigsResponse afterDeleteResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> serviceStub.getEntityDerivationConfigs(getRequest));

    assertEquals(0, afterDeleteResponse.getEntityDerivationConfigsCount());
  }

  @Test
  void testUpdateFailsOnNonExistentConfig() {
    UpdateEntityDerivationConfigRequest updateRequest =
        UpdateEntityDerivationConfigRequest.newBuilder()
            .setId("non-existent-id")
            .setData(createValidConfigData("Test"))
            .build();

    try {
      RequestContext.forTenantId(TENANT_ID)
          .call(() -> serviceStub.updateEntityDerivationConfig(updateRequest));
      fail("Expected update to fail");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
      assertTrue(e.getMessage().contains("No entity derivation config found"));
    }
  }

  @Test
  void testFilterByDisabled() {
    // Create enabled config
    CreateEntityDerivationConfigRequest enabledRequest =
        CreateEntityDerivationConfigRequest.newBuilder()
            .setData(createValidConfigData("Enabled Entity").toBuilder().setDisabled(false).build())
            .build();

    CreateEntityDerivationConfigResponse enabledResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> serviceStub.createEntityDerivationConfig(enabledRequest));

    // Create disabled config
    CreateEntityDerivationConfigRequest disabledRequest =
        CreateEntityDerivationConfigRequest.newBuilder()
            .setData(createValidConfigData("Disabled Entity").toBuilder().setDisabled(true).build())
            .build();

    RequestContext.forTenantId(TENANT_ID)
        .call(() -> serviceStub.createEntityDerivationConfig(disabledRequest));

    // Get without including disabled
    GetEntityDerivationConfigsRequest request =
        GetEntityDerivationConfigsRequest.newBuilder()
            .setFilter(EntityDerivationConfigFilter.newBuilder().setIncludeDisabled(false).build())
            .build();

    GetEntityDerivationConfigsResponse response =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> serviceStub.getEntityDerivationConfigs(request));

    assertEquals(1, response.getEntityDerivationConfigsCount());
    assertEquals(
        "Enabled Entity", response.getEntityDerivationConfigs(0).getData().getDisplayName());
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
