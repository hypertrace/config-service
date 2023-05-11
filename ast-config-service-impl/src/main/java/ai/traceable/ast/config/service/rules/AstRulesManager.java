package ai.traceable.ast.config.service.rules;

import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigRequest;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.VulnerabilityMetadataOverrides;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
class AstRulesManager implements RulesManager {
  private final ScanPurgeConfigStore scanPurgeConfigStore;
  private final VulnerabilityMetadataOverridesStore vulnerabilityMetadataOverridesStore;

  @Override
  public ScanPurgeConfig updateScanPurgeConfig(
      RequestContext requestContext, UpdateScanPurgeConfigRequest request) {
    ScanPurgeConfig scanPurgeConfig =
        ScanPurgeConfig.newBuilder()
            .setPurgeDuration(request.getPurgeConfig().getPurgeDuration())
            .build();
    return scanPurgeConfigStore.upsertObject(requestContext, scanPurgeConfig).getData();
  }

  @Override
  public Optional<ScanPurgeConfig> getScanPurgeConfig(RequestContext requestContext) {
    return scanPurgeConfigStore.getData(requestContext);
  }

  @Override
  public VulnerabilityMetadataOverrides updateVulnerabilityMetadataOverridesConfig(
      RequestContext requestContext, EditVulnerabilityMetadataOverridesRequest request) {
    VulnerabilityMetadataOverrides.Builder newVulnerabilityMetadataOverridesBuilder =
        VulnerabilityMetadataOverrides.newBuilder(
            vulnerabilityMetadataOverridesStore
                .getData(
                    requestContext,
                    request
                        .getVulnerabilityMetadataOverrides()
                        .getIdentifyingAttributes()
                        .getMetadataId())
                .orElse(VulnerabilityMetadataOverrides.newBuilder().build()));
    updateVulnerabilityMetadataOverrides(
        newVulnerabilityMetadataOverridesBuilder, request.getVulnerabilityMetadataOverrides());
    return vulnerabilityMetadataOverridesStore
        .upsertObject(requestContext, newVulnerabilityMetadataOverridesBuilder.build())
        .getData();
  }

  @Override
  public Optional<VulnerabilityMetadataOverrides> getVulnerabilityMetadataOverridesConfig(
      RequestContext requestContext, GetVulnerabilityMetadataOverridesRequest request) {
    return vulnerabilityMetadataOverridesStore.getData(requestContext, request.getMetadataId());
  }

  @Override
  public Optional<VulnerabilityMetadataOverrides> deleteVulnerabilityMetadataOverridesConfig(
      RequestContext requestContext, DeleteVulnerabilityMetadataOverridesConfigRequest request) {
    log.info("Resetting plugin to default with metadata Id: {}", request.getMetadataId());
    return vulnerabilityMetadataOverridesStore
        .deleteObject(requestContext, request.getMetadataId())
        .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()))
        .getDeletedData();
  }

  public void updateVulnerabilityMetadataOverrides(
      VulnerabilityMetadataOverrides.Builder existingVulnerabilityMetadata,
      VulnerabilityMetadataOverrides overriddenVulnerabilityMetadata) {
    existingVulnerabilityMetadata.setIdentifyingAttributes(
        overriddenVulnerabilityMetadata.getIdentifyingAttributes());
    if (overriddenVulnerabilityMetadata.getCustomerDefinedTagsCount() > 0) {
      existingVulnerabilityMetadata.clearCustomerDefinedTags();
      existingVulnerabilityMetadata.putAllCustomerDefinedTags(
          overriddenVulnerabilityMetadata.getCustomerDefinedTagsMap());
    }
    if (overriddenVulnerabilityMetadata.hasCvssScore()) {
      existingVulnerabilityMetadata.setCvssScore(overriddenVulnerabilityMetadata.getCvssScore());
    }
    if (overriddenVulnerabilityMetadata.hasSeverity()) {
      existingVulnerabilityMetadata.setSeverity(overriddenVulnerabilityMetadata.getSeverity());
    }
    if (overriddenVulnerabilityMetadata.hasCvssVectorString()) {
      existingVulnerabilityMetadata.setCvssVectorString(
          overriddenVulnerabilityMetadata.getCvssVectorString());
    }
  }
}
