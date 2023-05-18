package ai.traceable.ast.config.service.rules;

import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigRequest;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.IdentifyingAttributes;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import io.grpc.Status;
import org.hypertrace.config.validation.GrpcValidatorUtils;
import org.hypertrace.core.grpcutils.context.RequestContext;

class AstRulesValidator implements RulesValidator {

  @Override
  public void validateOrThrow(RequestContext requestContext, UpdateScanPurgeConfigRequest request) {
    validateOrThrow(requestContext);
    if (!request.hasPurgeConfig() || !request.getPurgeConfig().hasPurgeDuration()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have a valid purge config with a valid duration")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  @Override
  public void validateOrThrow(RequestContext requestContext, GetScanPurgeConfigRequest request) {
    validateOrThrow(requestContext);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, EditVulnerabilityMetadataOverridesRequest request) {
    validateOrThrow(requestContext);
    IdentifyingAttributes identifyingAttributes =
        request.getVulnerabilityMetadataOverrides().getIdentifyingAttributes();
    if (!request.getVulnerabilityMetadataOverrides().hasIdentifyingAttributes()
        || identifyingAttributes.getMetadataId().isEmpty()
        || identifyingAttributes.getCategory().isEmpty()
        || identifyingAttributes.getSubcategory().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have identifying attributes to update the Plugin")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, GetVulnerabilityMetadataOverridesRequest request) {
    validateOrThrow(requestContext);
    if (request.getMetadataId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have metadata_id to update the Plugin")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, DeleteVulnerabilityMetadataOverridesConfigRequest request) {
    validateOrThrow(requestContext);
    if (request.getMetadataId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have metadata_id to update the Plugin")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, GetAllVulnerabilityMetadataOverridesRequest request) {
    validateOrThrow(requestContext);
  }

  private void validateOrThrow(RequestContext requestContext) {
    GrpcValidatorUtils.validateRequestContextOrThrow(requestContext);
  }
}
