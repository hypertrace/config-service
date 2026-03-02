package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityCategory;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanProjection;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import io.grpc.StatusException;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntityDerivationConfigStoreManagerTest {

  @Mock private EntityDerivationConfigStore store;
  @Mock private UuidGenerator uuidGenerator;
  @Mock private DefaultEntityDerivationProvider defaultEntityDerivationProvider;
  @Mock private ContextualConfigObject<EntityDerivationConfig> contextualConfigObject;

  private EntityDerivationConfigStoreManager manager;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    manager =
        new EntityDerivationConfigStoreManager(
            store, uuidGenerator, defaultEntityDerivationProvider);
    requestContext = RequestContext.forTenantId("test-tenant");
  }

  @Test
  void testCreateEntityDerivationConfig_Success() {
    String generatedId = "config-123";
    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder()
            .setData(createValidConfigData("User Email Address"))
            .build();

    EntityDerivationConfig expectedConfig =
        EntityDerivationConfig.newBuilder()
            .setId(generatedId)
            .setColumnName("user_email_address")
            .setData(request.getData())
            .build();

    when(uuidGenerator.generateRandomId()).thenReturn(generatedId);
    when(store.upsertObject(eq(requestContext), any(EntityDerivationConfig.class)))
        .thenReturn(contextualConfigObject);
    when(contextualConfigObject.getData()).thenReturn(expectedConfig);

    CreateEntityDerivationConfigResponse response =
        manager.createEntityDerivationConfig(requestContext, request);

    assertNotNull(response);
    assertEquals("user_email_address", response.getEntityDerivationConfig().getColumnName());

    ArgumentCaptor<EntityDerivationConfig> configCaptor =
        ArgumentCaptor.forClass(EntityDerivationConfig.class);
    verify(store).upsertObject(eq(requestContext), configCaptor.capture());
    assertEquals("user_email_address", configCaptor.getValue().getColumnName());
  }

  @Test
  void testUpdateEntityDerivationConfig_Success() throws StatusException {
    String configId = "config-123";
    UpdateEntityDerivationConfigRequest request =
        UpdateEntityDerivationConfigRequest.newBuilder()
            .setId(configId)
            .setData(createValidConfigData("Updated Name"))
            .build();

    EntityDerivationConfig existingConfig =
        EntityDerivationConfig.newBuilder()
            .setId(configId)
            .setData(createValidConfigData("Old Name"))
            .build();

    EntityDerivationConfig updatedConfig =
        EntityDerivationConfig.newBuilder()
            .setId(configId)
            .setColumnName("updated_name")
            .setData(request.getData())
            .build();

    when(store.getData(requestContext, configId)).thenReturn(Optional.of(existingConfig));
    when(store.upsertObject(eq(requestContext), any(EntityDerivationConfig.class)))
        .thenReturn(contextualConfigObject);
    when(contextualConfigObject.getData()).thenReturn(updatedConfig);

    manager.updateEntityDerivationConfig(requestContext, request);

    ArgumentCaptor<EntityDerivationConfig> configCaptor =
        ArgumentCaptor.forClass(EntityDerivationConfig.class);
    verify(store).upsertObject(eq(requestContext), configCaptor.capture());
    assertEquals("updated_name", configCaptor.getValue().getColumnName());
  }

  @Test
  void testUpdateEntityDerivationConfig_ConfigNotFound() {
    String configId = "non-existent-config";
    UpdateEntityDerivationConfigRequest request =
        UpdateEntityDerivationConfigRequest.newBuilder()
            .setId(configId)
            .setData(createValidConfigData("Test"))
            .build();

    when(store.getData(requestContext, configId)).thenReturn(Optional.empty());

    StatusException exception =
        assertThrows(
            StatusException.class,
            () -> manager.updateEntityDerivationConfig(requestContext, request));
    assertEquals("NOT_FOUND", exception.getStatus().getCode().name());
  }

  @Test
  void testDeleteEntityDerivationConfig_Success() throws StatusException {
    String configId = "config-123";
    DeleteEntityDerivationConfigRequest request =
        DeleteEntityDerivationConfigRequest.newBuilder()
            .setEntityDerivationConfigId(configId)
            .build();

    EntityDerivationConfig existingConfig =
        EntityDerivationConfig.newBuilder()
            .setId(configId)
            .setData(createValidConfigData("Test"))
            .build();

    when(store.getData(requestContext, configId)).thenReturn(Optional.of(existingConfig));

    manager.deleteEntityDerivationConfig(requestContext, request);

    verify(store).deleteObject(requestContext, configId);
  }

  private EntityDerivationConfigData createValidConfigData(String displayName) {
    return EntityDerivationConfigData.newBuilder()
        .setDisplayName(displayName)
        .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
        .setEventKind(ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
        .setSpanProjection(
            SpanProjection.newBuilder()
                .addEventDerivationConfigs(
                    EventDerivationConfigDetails.newBuilder().setName("Test Derivation Rule")))
        .build();
  }
}
