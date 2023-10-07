package ai.traceable.ast.config.service.rules;

import ai.traceable.ast.config.service.v1.AstFeatureConfig;
import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigRequest;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAstFeatureConfigsRequest;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.UpdateAstFeatureConfigRequest;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.VulnerabilityMetadataOverrides;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesManager {
  ScanPurgeConfig updateScanPurgeConfig(
      RequestContext requestContext, UpdateScanPurgeConfigRequest request);

  Optional<ScanPurgeConfig> getScanPurgeConfig(RequestContext requestContext);

  VulnerabilityMetadataOverrides updateVulnerabilityMetadataOverridesConfig(
      RequestContext requestContext, EditVulnerabilityMetadataOverridesRequest request);

  Optional<VulnerabilityMetadataOverrides> getVulnerabilityMetadataOverridesConfig(
      RequestContext requestContext, GetVulnerabilityMetadataOverridesRequest request);

  Optional<VulnerabilityMetadataOverrides> deleteVulnerabilityMetadataOverridesConfig(
      RequestContext requestContext, DeleteVulnerabilityMetadataOverridesConfigRequest request);

  List<VulnerabilityMetadataOverrides> getAllVulnerabilityMetadataOverridesConfig(
      RequestContext requestContext, GetAllVulnerabilityMetadataOverridesRequest request);

  List<AstFeatureConfig> getAstFeatureConfigs(
      RequestContext requestContext, GetAstFeatureConfigsRequest request);

  AstFeatureConfig updateAstFeatureConfig(
      RequestContext requestContext, UpdateAstFeatureConfigRequest request);
}
