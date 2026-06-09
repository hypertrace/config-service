package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityCategory;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EnvironmentScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocation;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocationType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ParentDerivation;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.Scope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanBasedExtraction;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanProjection;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.event.kind.DefaultFraudDataModelEventKindRegistry;
import ai.traceable.fraud.datamodel.event.kind.aggregationfunction.DefaultAggregationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.DefaultEventKindProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.operator.DefaultOperatorProvider;
import ai.traceable.fraud.datamodel.event.kind.transformationfunction.DefaultTransformationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.FraudDataModelEventKindRegistry;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionInvocation;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationPipeline;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntityDerivationConfigRequestValidatorTest {

  @Mock private FraudDataModelEventKindRegistry fraudDataModelEventKindRegistry;

  private EntityDerivationConfigRequestValidator validator;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    DefaultEntityDerivationProvider defaultEntityDerivationProvider =
        new DefaultEntityDerivationProvider();
    validator =
        new EntityDerivationConfigRequestValidator(
            defaultEntityDerivationProvider, fraudDataModelEventKindRegistry);
    requestContext = RequestContext.forTenantId("test-tenant");
  }

  @Test
  void testValidateCreateRequest_Success() {
    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder().setData(createValidConfigData()).build();

    assertDoesNotThrow(
        () -> validator.validateCreateRequest(request, Optional.empty(), requestContext));
  }

  @Test
  void testValidateCreateRequest_MissingData() {
    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder().build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, Optional.empty(), requestContext));
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

    assertDoesNotThrow(
        () -> validator.validateUpdateRequest(request, Optional.empty(), requestContext));
  }

  @Test
  void testValidateUpdateRequest_MissingId() {
    UpdateEntityDerivationConfigRequest request =
        UpdateEntityDerivationConfigRequest.newBuilder().setData(createValidConfigData()).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateUpdateRequest(request, Optional.empty(), requestContext));
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
            () ->
                validator.validateCreateRequest(
                    requestWithEmptyDisplayName, Optional.empty(), requestContext));
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
            () ->
                validator.validateCreateRequest(
                    requestWithUnspecifiedCategory, Optional.empty(), requestContext));
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
                    Optional.empty(),
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
                    Optional.empty(),
                    requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Span projection must contain at least one event derivation config",
        exception.getMessage());
  }

  @Test
  void testValidateCreateRequest_ParentDerivationWithValidParent_Success() {
    when(fraudDataModelEventKindRegistry.isKindCompatible(any(), any())).thenReturn(true);

    EntityDerivationConfigData dataWithParentDerivation =
        EntityDerivationConfigData.newBuilder()
            .setDisplayName("Derived Entity")
            .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
            .setEventKind(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
            .setParentDerivation(
                ParentDerivation.newBuilder().setParentEntityDerivationId("parent-entity-id"))
            .build();

    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder().setData(dataWithParentDerivation).build();

    ComplexDataModelEventKind parentEventKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();

    assertDoesNotThrow(
        () ->
            validator.validateCreateRequest(request, Optional.of(parentEventKind), requestContext));
  }

  @Test
  void testValidateCreateRequest_ParentDerivationWithIncompatibleType_ThrowsError() {
    when(fraudDataModelEventKindRegistry.isKindCompatible(any(), any())).thenReturn(false);

    EntityDerivationConfigData dataWithParentDerivation =
        EntityDerivationConfigData.newBuilder()
            .setDisplayName("Derived Entity")
            .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
            .setEventKind(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_long"))
            .setParentDerivation(
                ParentDerivation.newBuilder().setParentEntityDerivationId("parent-entity-id"))
            .build();

    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder().setData(dataWithParentDerivation).build();

    ComplexDataModelEventKind parentEventKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                validator.validateCreateRequest(
                    request, Optional.of(parentEventKind), requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Parent pipeline output type 'system_event_kind_string' is not assignable to child entity type 'system_event_kind_long' (output must be the same kind or a subtype of the child kind)",
        exception.getMessage());
  }

  @Test
  void testValidateCreateRequest_ParentDerivationWithNonExistentParent_ThrowsError() {
    EntityDerivationConfigData dataWithParentDerivation =
        EntityDerivationConfigData.newBuilder()
            .setDisplayName("Derived Entity")
            .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
            .setEventKind(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
            .setParentDerivation(
                ParentDerivation.newBuilder().setParentEntityDerivationId("non-existent-parent"))
            .build();

    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder().setData(dataWithParentDerivation).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, Optional.empty(), requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Parent entity derivation config not found: non-existent-parent",
        exception.getMessage());
  }

  @Test
  void testValidateCreateRequest_SpanProjectionMissingScope_ThrowsError() {
    EntityDerivationConfigData dataWithMissingScope =
        EntityDerivationConfigData.newBuilder()
            .setDisplayName("Test Entity")
            .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
            .setEventKind(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
            .setSpanProjection(
                SpanProjection.newBuilder()
                    .addEventDerivationConfigs(
                        EventDerivationConfigDetails.newBuilder()
                            .setSpanExtraction(
                                SpanBasedExtraction.newBuilder()
                                    .setLocation(
                                        ExtractionLocation.newBuilder()
                                            .setLocationType(
                                                ExtractionLocationType
                                                    .EXTRACTION_LOCATION_TYPE_REQUEST_HEADER)
                                            .setKey("X-User-Id")))))
            .build();

    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder().setData(dataWithMissingScope).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, Optional.empty(), requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Event derivation config[0]: Scope is required", exception.getMessage());
  }

  @Test
  void testValidateCreateRequest_SpanProjectionMissingExtraction_ThrowsError() {
    EntityDerivationConfigData dataWithMissingExtraction =
        EntityDerivationConfigData.newBuilder()
            .setDisplayName("Test Entity")
            .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
            .setEventKind(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
            .setSpanProjection(
                SpanProjection.newBuilder()
                    .addEventDerivationConfigs(
                        EventDerivationConfigDetails.newBuilder()
                            .setScope(
                                Scope.newBuilder()
                                    .setEnvironmentScope(EnvironmentScope.newBuilder()))))
            .build();

    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder().setData(dataWithMissingExtraction).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> validator.validateCreateRequest(request, Optional.empty(), requestContext));
    assertEquals(
        "INVALID_ARGUMENT: Event derivation config[0]: Extraction method is required (span_extraction or jexl_expression)",
        exception.getMessage());
  }

  @Test
  void spanProjection_withRealRegistry_emailEntity_rejectsPipelineEndingAsString() {
    FraudDataModelEventKindRegistry registry = realEventKindRegistry();
    EntityDerivationConfigRequestValidator realValidator =
        new EntityDerivationConfigRequestValidator(new DefaultEntityDerivationProvider(), registry);

    EntityDerivationConfigData data =
        EntityDerivationConfigData.newBuilder()
            .setDisplayName("Test Entity")
            .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
            .setEventKind(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_email"))
            .setSpanProjection(
                SpanProjection.newBuilder()
                    .addEventDerivationConfigs(
                        EventDerivationConfigDetails.newBuilder()
                            .setName("rule")
                            .setScope(
                                Scope.newBuilder()
                                    .setEnvironmentScope(EnvironmentScope.newBuilder()))
                            .setSpanExtraction(
                                SpanBasedExtraction.newBuilder()
                                    .setLocation(
                                        ExtractionLocation.newBuilder()
                                            .setLocationType(
                                                ExtractionLocationType
                                                    .EXTRACTION_LOCATION_TYPE_REQUEST_HEADER)
                                            .setKey("X-User")))
                            .setPipeline(
                                TransformationPipeline.newBuilder()
                                    .addTransformationPipeline(
                                        TransformationFunctionInvocation.newBuilder()
                                            .setFunctionId("type_cast_to_system_event_kind_email"))
                                    .addTransformationPipeline(
                                        TransformationFunctionInvocation.newBuilder()
                                            .setFunctionId(
                                                "type_cast_to_system_event_kind_string")))))
            .build();

    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder().setData(data).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> realValidator.validateCreateRequest(request, Optional.empty(), requestContext));
    assertTrue(exception.getMessage().contains("not assignable"));
  }

  @Test
  void spanProjection_withRealRegistry_emailEntity_acceptsPipelineEndingAsEmail() {
    FraudDataModelEventKindRegistry registry = realEventKindRegistry();
    EntityDerivationConfigRequestValidator realValidator =
        new EntityDerivationConfigRequestValidator(new DefaultEntityDerivationProvider(), registry);

    EntityDerivationConfigData data =
        EntityDerivationConfigData.newBuilder()
            .setDisplayName("Test Entity")
            .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
            .setEventKind(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_email"))
            .setSpanProjection(
                SpanProjection.newBuilder()
                    .addEventDerivationConfigs(
                        EventDerivationConfigDetails.newBuilder()
                            .setName("rule")
                            .setScope(
                                Scope.newBuilder()
                                    .setEnvironmentScope(EnvironmentScope.newBuilder()))
                            .setSpanExtraction(
                                SpanBasedExtraction.newBuilder()
                                    .setLocation(
                                        ExtractionLocation.newBuilder()
                                            .setLocationType(
                                                ExtractionLocationType
                                                    .EXTRACTION_LOCATION_TYPE_REQUEST_HEADER)
                                            .setKey("X-User")))
                            .setPipeline(
                                TransformationPipeline.newBuilder()
                                    .addTransformationPipeline(
                                        TransformationFunctionInvocation.newBuilder()
                                            .setFunctionId(
                                                "type_cast_to_system_event_kind_email")))))
            .build();

    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder().setData(data).build();

    assertDoesNotThrow(
        () -> realValidator.validateCreateRequest(request, Optional.empty(), requestContext));
  }

  private static FraudDataModelEventKindRegistry realEventKindRegistry() {
    DefaultEventKindProvider kinds = new DefaultEventKindProvider();
    EventKindHierarchyResolver hierarchy = new EventKindHierarchyResolver(kinds);
    return new DefaultFraudDataModelEventKindRegistry(
        hierarchy,
        new DefaultTransformationFunctionProvider(hierarchy, kinds),
        new DefaultOperatorProvider(hierarchy),
        new DefaultAggregationFunctionProvider(hierarchy));
  }

  private EntityDerivationConfigData createValidConfigData() {
    return EntityDerivationConfigData.newBuilder()
        .setDisplayName("Test Entity")
        .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
        .setEventKind(ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
        .setSpanProjection(
            SpanProjection.newBuilder()
                .addEventDerivationConfigs(
                    EventDerivationConfigDetails.newBuilder()
                        .setName("Test Derivation Rule")
                        .setScope(
                            Scope.newBuilder().setEnvironmentScope(EnvironmentScope.newBuilder()))
                        .setSpanExtraction(
                            SpanBasedExtraction.newBuilder()
                                .setLocation(
                                    ExtractionLocation.newBuilder()
                                        .setLocationType(
                                            ExtractionLocationType
                                                .EXTRACTION_LOCATION_TYPE_REQUEST_HEADER)
                                        .setKey("X-User-Id")))))
        .build();
  }
}
