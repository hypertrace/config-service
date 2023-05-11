package ai.traceable.ast.config.service.rules;

import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigRequest;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesValidator {
  void validateOrThrow(RequestContext requestContext, UpdateScanPurgeConfigRequest request);

  void validateOrThrow(RequestContext requestContext, GetScanPurgeConfigRequest request);

  void validateOrThrow(
      RequestContext requestContext, EditVulnerabilityMetadataOverridesRequest request);

  void validateOrThrow(
      RequestContext requestContext, GetVulnerabilityMetadataOverridesRequest request);

  void validateOrThrow(
      RequestContext requestContext, DeleteVulnerabilityMetadataOverridesConfigRequest request);
}
