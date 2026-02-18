package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityCategory;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EntityDerivationConfigRequestValidator {

  public void validateRequestContext(RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    if (requestContext.getTenantId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing expected Tenant ID")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  public void validateCreateRequest(
      CreateEntityDerivationConfigRequest request, RequestContext requestContext) {
    validateRequestContext(requestContext);

    if (!request.hasData()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Entity derivation configuration data is required")
          .asRuntimeException(requestContext.buildTrailers());
    }

    validateEntityDerivationConfigData(request.getData(), requestContext);
  }

  public void validateUpdateRequest(
      UpdateEntityDerivationConfigRequest request, RequestContext requestContext) {
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

    validateEntityDerivationConfigData(request.getData(), requestContext);
  }

  public void validateDeleteRequest(
      DeleteEntityDerivationConfigRequest request, RequestContext requestContext) {
    validateRequestContext(requestContext);

    if (request.getEntityDerivationConfigId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Entity derivation config ID is required for deletion")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateEntityDerivationConfigData(
      EntityDerivationConfigData data, RequestContext requestContext) {
    validateDisplayName(data, requestContext);
    validateCategory(data, requestContext);
    validateEventKind(data, requestContext);
    validateValueSource(data, requestContext);
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

  private void validateValueSource(EntityDerivationConfigData data, RequestContext requestContext) {
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
      validateParentDerivation(data, requestContext);
    }
  }

  private void validateSpanProjection(
      EntityDerivationConfigData data, RequestContext requestContext) {
    if (data.getSpanProjection().getEventDerivationConfigsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Span projection must contain at least one event derivation config")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateParentDerivation(
      EntityDerivationConfigData data, RequestContext requestContext) {
    if (data.getParentDerivation().getParentEntityDerivationId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Parent entity derivation ID is required for parent derivation")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }
}
