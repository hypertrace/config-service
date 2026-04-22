package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityCategory;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ParentDerivation;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.Scope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanProjection;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.FraudDataModelEventKindRegistry;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationPipeline;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EntityDerivationConfigRequestValidator {

  private final DefaultEntityDerivationProvider defaultEntityDerivationProvider;
  private final FraudDataModelEventKindRegistry fraudDataModelEventKindRegistry;

  @Inject
  public EntityDerivationConfigRequestValidator(
      DefaultEntityDerivationProvider defaultEntityDerivationProvider,
      FraudDataModelEventKindRegistry fraudDataModelEventKindRegistry) {
    this.defaultEntityDerivationProvider = defaultEntityDerivationProvider;
    this.fraudDataModelEventKindRegistry = fraudDataModelEventKindRegistry;
  }

  public void validateRequestContext(RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    if (requestContext.getTenantId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing expected Tenant ID")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  public void validateCreateRequest(
      CreateEntityDerivationConfigRequest request,
      Optional<ComplexDataModelEventKind> parentEventKind,
      RequestContext requestContext) {
    validateRequestContext(requestContext);

    if (!request.hasData()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Entity derivation configuration data is required")
          .asRuntimeException(requestContext.buildTrailers());
    }

    validateEntityDerivationConfigData(request.getData(), parentEventKind, requestContext);
  }

  public void validateUpdateRequest(
      UpdateEntityDerivationConfigRequest request,
      Optional<ComplexDataModelEventKind> parentEventKind,
      RequestContext requestContext) {
    validateRequestContext(requestContext);

    if (request.getId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Entity derivation config ID is required for update")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (!request.hasData()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Entity derivation configuration data is required")
          .asRuntimeException(requestContext.buildTrailers());
    }

    validateSystemEntityUpdate(request.getId(), requestContext);
    validateMandatoryEntityUpdate(request, requestContext);
    validateEntityDerivationConfigData(request.getData(), parentEventKind, requestContext);
  }

  public void validateDeleteRequest(
      DeleteEntityDerivationConfigRequest request, RequestContext requestContext) {
    validateRequestContext(requestContext);

    if (request.getEntityDerivationConfigId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Entity derivation config ID is required for deletion")
          .asRuntimeException(requestContext.buildTrailers());
    }

    validateSystemEntityDeletion(request.getEntityDerivationConfigId(), requestContext);
    validateMandatoryEntityDeletion(request.getEntityDerivationConfigId(), requestContext);
  }

  private void validateEntityDerivationConfigData(
      EntityDerivationConfigData data,
      Optional<ComplexDataModelEventKind> parentEventKind,
      RequestContext requestContext) {
    validateDisplayName(data, requestContext);
    validateCategory(data, requestContext);
    validateEventKind(data, requestContext);
    validateValueSource(data, parentEventKind, requestContext);
  }

  private void validateDisplayName(EntityDerivationConfigData data, RequestContext requestContext) {
    if (data.getDisplayName().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Display name is required")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateCategory(EntityDerivationConfigData data, RequestContext requestContext) {
    if (data.getCategory() == EntityCategory.ENTITY_CATEGORY_UNSPECIFIED) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Entity category must be specified")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateEventKind(EntityDerivationConfigData data, RequestContext requestContext) {
    if (!data.hasEventKind()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Event kind is required")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateValueSource(
      EntityDerivationConfigData data,
      Optional<ComplexDataModelEventKind> parentEventKind,
      RequestContext requestContext) {
    boolean hasSpanProjection = data.hasSpanProjection();
    boolean hasParentDerivation = data.hasParentDerivation();

    // Exactly one of span_projection or parent_derivation must be set
    if (!hasSpanProjection && !hasParentDerivation) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Value source is required: exactly one of span_projection or parent_derivation must be set")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (hasSpanProjection && hasParentDerivation) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Only one value source can be set: either span_projection or parent_derivation, not both")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // Validate span_projection if present
    if (hasSpanProjection) {
      validateSpanProjection(data, requestContext);
    }

    // Validate parent_derivation if present
    if (hasParentDerivation) {
      validateParentDerivation(data, parentEventKind, requestContext);
    }
  }

  private void validateSpanProjection(
      EntityDerivationConfigData data, RequestContext requestContext) {
    SpanProjection spanProjection = data.getSpanProjection();
    if (spanProjection.getEventDerivationConfigsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Span projection must contain at least one event derivation config")
          .asRuntimeException(requestContext.buildTrailers());
    }

    ComplexDataModelEventKind entityEventKind = data.getEventKind();
    int index = 0;
    for (EventDerivationConfigDetails details : spanProjection.getEventDerivationConfigsList()) {
      validateEventDerivationConfigDetails(details, entityEventKind, index++, requestContext);
    }
  }

  private void validateEventDerivationConfigDetails(
      EventDerivationConfigDetails details,
      ComplexDataModelEventKind entityEventKind,
      int index,
      RequestContext requestContext) {
    String prefix = "Event derivation config[" + index + "]: ";

    validateEventDerivationScope(details, prefix, requestContext);
    validateEventDerivationExtraction(details, prefix, requestContext);
    validateEventDerivationPipeline(details, entityEventKind, prefix, requestContext);
  }

  private void validateEventDerivationScope(
      EventDerivationConfigDetails details, String prefix, RequestContext requestContext) {
    if (!details.hasScope()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(prefix + "Scope is required")
          .asRuntimeException(requestContext.buildTrailers());
    }

    Scope scope = details.getScope();
    if (!scope.hasEnvironmentScope()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(prefix + "Environment scope is required")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateEventDerivationExtraction(
      EventDerivationConfigDetails details, String prefix, RequestContext requestContext) {
    boolean hasSpanExtraction = details.hasSpanExtraction();
    boolean hasJexlExpression =
        details.hasJexlExpression() && !details.getJexlExpression().isEmpty();

    if (!hasSpanExtraction && !hasJexlExpression) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              prefix + "Extraction method is required (span_extraction or jexl_expression)")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateEventDerivationPipeline(
      EventDerivationConfigDetails details,
      ComplexDataModelEventKind entityEventKind,
      String prefix,
      RequestContext requestContext) {
    if (!details.hasPipeline() || details.getPipeline().getTransformationPipelineCount() == 0) {
      return;
    }

    TransformationPipeline pipeline = details.getPipeline();
    ComplexDataModelEventKind spanExtractionKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();

    try {
      ComplexDataModelEventKind outputKind =
          fraudDataModelEventKindRegistry.validateTransformationPipeline(
              spanExtractionKind, pipeline);

      if (!fraudDataModelEventKindRegistry.isKindCompatible(entityEventKind, outputKind)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                prefix
                    + String.format(
                        "Pipeline output type '%s' is not assignable to entity type '%s' "
                            + "(output must be the same kind or a subtype of the entity kind)",
                        formatKind(outputKind), formatKind(entityEventKind)))
            .asRuntimeException(requestContext.buildTrailers());
      }
    } catch (IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(prefix + e.getMessage())
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateParentDerivation(
      EntityDerivationConfigData data,
      Optional<ComplexDataModelEventKind> parentEventKind,
      RequestContext requestContext) {
    ParentDerivation parentDerivation = data.getParentDerivation();
    String parentId = parentDerivation.getParentEntityDerivationId();
    if (parentId.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Parent entity derivation ID is required for parent derivation")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (parentEventKind.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Parent entity derivation config not found: " + parentId)
          .asRuntimeException(requestContext.buildTrailers());
    }

    ComplexDataModelEventKind parentKind = parentEventKind.get();
    ComplexDataModelEventKind childKind = data.getEventKind();
    TransformationPipeline pipeline =
        parentDerivation.hasPipeline() ? parentDerivation.getPipeline() : null;

    validateParentChildTypeCompatibility(parentKind, childKind, pipeline, requestContext);
  }

  private void validateParentChildTypeCompatibility(
      ComplexDataModelEventKind parentKind,
      ComplexDataModelEventKind childKind,
      TransformationPipeline pipeline,
      RequestContext requestContext) {
    ComplexDataModelEventKind effectiveParentKind = parentKind;

    if (pipeline != null && pipeline.getTransformationPipelineCount() > 0) {
      try {
        effectiveParentKind =
            fraudDataModelEventKindRegistry.validateTransformationPipeline(parentKind, pipeline);
      } catch (IllegalArgumentException e) {
        throw Status.INVALID_ARGUMENT
            .withDescription(e.getMessage())
            .asRuntimeException(requestContext.buildTrailers());
      }
    }

    if (!fraudDataModelEventKindRegistry.isKindCompatible(childKind, effectiveParentKind)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Parent pipeline output type '%s' is not assignable to child entity type '%s' "
                      + "(output must be the same kind or a subtype of the child kind)",
                  formatKind(effectiveParentKind), formatKind(childKind)))
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private String formatKind(ComplexDataModelEventKind kind) {
    if (kind.hasKindId()) {
      return kind.getKindId();
    }
    if (kind.hasArrayOf()) {
      return "array<" + formatKind(kind.getArrayOf()) + ">";
    }
    return kind.toString();
  }

  private void validateMandatoryEntityDeletion(String entityId, RequestContext requestContext) {
    validateDefaultEntityByCategory(
        entityId,
        EntityCategory.ENTITY_CATEGORY_MANDATORY,
        "Cannot delete mandatory entity",
        requestContext);
  }

  private void validateMandatoryEntityUpdate(
      UpdateEntityDerivationConfigRequest request, RequestContext requestContext) {
    EntityDerivationConfig defaultEntity =
        defaultEntityDerivationProvider.getDefaultEntity(request.getId());
    if (defaultEntity != null
        && defaultEntity.getData().getCategory() == EntityCategory.ENTITY_CATEGORY_MANDATORY) {
      if (request.getData().getCategory() != EntityCategory.ENTITY_CATEGORY_MANDATORY) {
        throw Status.PERMISSION_DENIED
            .withDescription(
                "Cannot change category of mandatory entity: "
                    + defaultEntity.getData().getDisplayName()
                    + " (ID: "
                    + request.getId()
                    + ")")
            .asRuntimeException(requestContext.buildTrailers());
      }
    }
  }

  private void validateSystemEntityDeletion(String entityId, RequestContext requestContext) {
    validateDefaultEntityByCategory(
        entityId,
        EntityCategory.ENTITY_CATEGORY_SYSTEM,
        "Cannot delete system entity",
        requestContext);
  }

  private void validateSystemEntityUpdate(String entityId, RequestContext requestContext) {
    validateDefaultEntityByCategory(
        entityId,
        EntityCategory.ENTITY_CATEGORY_SYSTEM,
        "Cannot update system entity",
        requestContext);
  }

  private void validateDefaultEntityByCategory(
      String entityId, EntityCategory category, String errorPrefix, RequestContext requestContext) {
    EntityDerivationConfig defaultEntity =
        defaultEntityDerivationProvider.getDefaultEntity(entityId);
    if (defaultEntity != null && defaultEntity.getData().getCategory() == category) {
      String message =
          errorPrefix + ": " + defaultEntity.getData().getDisplayName() + " (ID: " + entityId + ")";
      if (category == EntityCategory.ENTITY_CATEGORY_SYSTEM) {
        message += ". System entities are read-only.";
      }
      throw Status.PERMISSION_DENIED
          .withDescription(message)
          .asRuntimeException(requestContext.buildTrailers());
    }
  }
}
