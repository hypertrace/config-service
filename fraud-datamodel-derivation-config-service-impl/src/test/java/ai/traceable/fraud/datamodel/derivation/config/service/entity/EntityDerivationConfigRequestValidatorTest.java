package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityCategory;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanProjection;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import io.grpc.StatusRuntimeException;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EntityDerivationConfigRequestValidatorTest {

  private EntityDerivationConfigRequestValidator validator;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    validator = new EntityDerivationConfigRequestValidator();
    requestContext = RequestContext.forTenantId("test-tenant");
  }

  @Test
  void testValidateCreateRequest_Success() {
    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder().setData(createValidConfigData()).build();

    assertDoesNotThrow(() -> validator.validateCreateRequest(request, requestContext));
  }

  @Test
  void testValidateCreateRequest_MissingData() {
    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder().build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Entity derivation configuration data is required",
        exception.getMessage());
  }

  @Test
  void testValidateUpdateRequest_Success() {
    UpdateEntityDerivationConfigRequest request =
        UpdateEntityDerivationConfigRequest.newBuilder()
            .setId("config-123")
            .setData(createValidConfigData())
            .build();

    assertDoesNotThrow(() -> validator.validateUpdateRequest(request, requestContext));
  }

  @Test
  void testValidateUpdateRequest_MissingId() {
    UpdateEntityDerivationConfigRequest request =
        UpdateEntityDerivationConfigRequest.newBuilder().setData(createValidConfigData()).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateUpdateRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Entity derivation config ID is required for update",
        exception.getMessage());
  }

  @Test
  void testValidateDeleteRequest_Success() {
    DeleteEntityDerivationConfigRequest request =
        DeleteEntityDerivationConfigRequest.newBuilder()
            .setEntityDerivationConfigId("config-123")
            .build();

    assertDoesNotThrow(() -> validator.validateDeleteRequest(request, requestContext));
  }

  @Test
  void testValidateDeleteRequest_MissingId() {
    DeleteEntityDerivationConfigRequest request =
        DeleteEntityDerivationConfigRequest.newBuilder().build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateDeleteRequest(request, requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Entity derivation config ID is required for deletion",
        exception.getMessage());
  }

  @Test
  void testValidateCreateRequest_InvalidDataFields() {
    CreateEntityDerivationConfigRequest requestWithEmptyDisplayName =
        CreateEntityDerivationConfigRequest.newBuilder()
            .setData(createValidConfigData().toBuilder().setDisplayName("").build())
            .build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(requestWithEmptyDisplayName, requestContext));
    assertEquals("INVALID_ARGUMENT: Display name is required", exception.getMessage());

    CreateEntityDerivationConfigRequest requestWithUnspecifiedCategory =
        CreateEntityDerivationConfigRequest.newBuilder()
            .setData(
                createValidConfigData().toBuilder()
                    .setCategory(EntityCategory.ENTITY_CATEGORY_UNSPECIFIED)
                    .build())
            .build();

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(requestWithUnspecifiedCategory, requestContext));
    assertEquals("INVALID_ARGUMENT: Entity category must be specified", exception.getMessage());
  }

  @Test
  void testValidateCreateRequest_InvalidValueSource() {
    EntityDerivationConfigData dataWithNoValueSource =
        EntityDerivationConfigData.newBuilder()
            .setDisplayName("Test Entity")
            .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
            .setEventKind(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
            .build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                validator.validateCreateRequest(
                    CreateEntityDerivationConfigRequest.newBuilder()
                        .setData(dataWithNoValueSource)
                        .build(),
                    requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Value source is required: exactly one of span_projection or parent_derivation must be set",
        exception.getMessage());

    EntityDerivationConfigData dataWithEmptySpanProjection =
        EntityDerivationConfigData.newBuilder()
            .setDisplayName("Test Entity")
            .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
            .setEventKind(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
            .setSpanProjection(SpanProjection.newBuilder())
            .build();

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                validator.validateCreateRequest(
                    CreateEntityDerivationConfigRequest.newBuilder()
                        .setData(dataWithEmptySpanProjection)
                        .build(),
                    requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Span projection must contain at least one event derivation config",
        exception.getMessage());
  }

  private EntityDerivationConfigData createValidConfigData() {
    return EntityDerivationConfigData.newBuilder()
        .setDisplayName("Test Entity")
        .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
        .setEventKind(ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
        .setSpanProjection(
            SpanProjection.newBuilder()
                .addEventDerivationConfigs(EventDerivationConfigDetails.newBuilder()))
        .build();
  }
}
