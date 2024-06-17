package ai.traceable.ast.config.service.validation;

import ai.traceable.ast.config.service.v1.CreateAstOverrideRequest;
import ai.traceable.ast.config.service.v1.CreateCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.DeleteAstOverridesRequest;
import ai.traceable.ast.config.service.v1.DeleteCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigRequest;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAllCustomTestPluginsRequest;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAstFeatureConfigsRequest;
import ai.traceable.ast.config.service.v1.GetAstOverridesRequest;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.UpdateAstFeatureConfigRequest;
import ai.traceable.ast.config.service.v1.UpdateAstOverrideRequest;
import ai.traceable.ast.config.service.v1.UpdateCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface AstConfigServiceRequestValidator {
  void validateOrThrow(RequestContext requestContext, UpdateScanPurgeConfigRequest request);

  void validateOrThrow(RequestContext requestContext, GetScanPurgeConfigRequest request);

  void validateOrThrow(
      RequestContext requestContext, EditVulnerabilityMetadataOverridesRequest request);

  void validateOrThrow(
      RequestContext requestContext, GetVulnerabilityMetadataOverridesRequest request);

  void validateOrThrow(
      RequestContext requestContext, DeleteVulnerabilityMetadataOverridesConfigRequest request);

  void validateOrThrow(
      RequestContext requestContext, GetAllVulnerabilityMetadataOverridesRequest request);

  void validateOrThrow(RequestContext requestContext, GetAstFeatureConfigsRequest request);

  void validateOrThrow(RequestContext requestContext, UpdateAstFeatureConfigRequest request);

  void validateOrThrow(RequestContext requestContext, GetAllCustomTestPluginsRequest request);

  void validateOrThrow(RequestContext requestContext, CreateCustomTestPluginRequest request);

  void validateOrThrow(RequestContext requestContext, UpdateCustomTestPluginRequest request);

  void validateOrThrow(RequestContext requestContext, DeleteCustomTestPluginRequest request);

  void validateOrThrow(RequestContext requestContext, GetAstOverridesRequest request);

  void validateOrThrow(RequestContext requestContext, CreateAstOverrideRequest request);

  void validateOrThrow(RequestContext requestContext, UpdateAstOverrideRequest request);

  void validateOrThrow(RequestContext requestContext, DeleteAstOverridesRequest request);
}
