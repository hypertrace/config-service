package ai.traceable.ast.config.service.rules;

import ai.traceable.ast.config.service.v1.CustomerDefinedTagsMap;
import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigRequest;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.IdentifyingAttributes;
import ai.traceable.ast.config.service.v1.TagValue;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.VulnerabilityMetadataOverrides;
import ai.traceable.ast.config.service.v1.VulnerabilitySeverity;
import com.google.protobuf.Duration;
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
    final VulnerabilityMetadataOverrides vulnerabilityMetadataOverrides =
        request.getVulnerabilityMetadataOverrides();
    IdentifyingAttributes identifyingAttributes =
        vulnerabilityMetadataOverrides.getIdentifyingAttributes();
    if (!vulnerabilityMetadataOverrides.hasIdentifyingAttributes()
        || identifyingAttributes.getMetadataId().isEmpty()
        || identifyingAttributes.getCategory().isEmpty()
        || identifyingAttributes.getSubcategory().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have identifying attributes to update the Plugin")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (vulnerabilityMetadataOverrides.hasSeverity()) {
      validateSeverity(requestContext, vulnerabilityMetadataOverrides.getSeverity());
    }
    if (vulnerabilityMetadataOverrides.hasCvssVectorString()) {
      validateCvssVectorString(
          requestContext, vulnerabilityMetadataOverrides.getCvssVectorString());
    }
    if (vulnerabilityMetadataOverrides.hasEstimatedFixTime()) {
      validateEstimatedFixTime(
          requestContext, vulnerabilityMetadataOverrides.getEstimatedFixTime());
    }
    if (vulnerabilityMetadataOverrides.hasCustomerDefinedTags()) {
      validateCustomerDefinedTags(
          requestContext, vulnerabilityMetadataOverrides.getCustomerDefinedTags());
    }
  }

  private void validateCustomerDefinedTags(
      final RequestContext requestContext, final CustomerDefinedTagsMap customerDefinedTags) {
    if (CustomerDefinedTagsMap.getDefaultInstance().equals(customerDefinedTags)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Customer defined tags provided: %s cannot be empty", customerDefinedTags))
          .asRuntimeException(requestContext.buildTrailers());
    }
    customerDefinedTags
        .getCustomerDefinedTagsMap()
        .entrySet()
        .forEach(entry -> validateTagValue(requestContext, entry.getKey(), entry.getValue()));
  }

  private void validateTagValue(
      final RequestContext requestContext, final String key, final TagValue value) {
    if (value.getValueCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Tag values set cannot be empty. Empty values were set for the key: %s", key))
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateEstimatedFixTime(
      final RequestContext requestContext, final Duration estimatedFixTime) {
    if (Duration.getDefaultInstance().equals(estimatedFixTime)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Invalid Estimated fix time provided: %s", estimatedFixTime))
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateCvssVectorString(
      final RequestContext requestContext, final String cvssVectorString) {
    if (cvssVectorString.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Cvss vector string provided cannot be blank")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateSeverity(
      final RequestContext requestContext, final VulnerabilitySeverity severity) {
    switch (severity) {
      case VULNERABILITY_SEVERITY_LOW:
      case VULNERABILITY_SEVERITY_MEDIUM:
      case VULNERABILITY_SEVERITY_HIGH:
      case VULNERABILITY_SEVERITY_CRITICAL:
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Encountered unknown vulnerability severity: %s", severity))
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
    if (request.getMetadataId().isBlank()) {
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
