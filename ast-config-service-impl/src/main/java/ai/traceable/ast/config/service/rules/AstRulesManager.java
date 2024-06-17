package ai.traceable.ast.config.service.rules;

import ai.traceable.ast.config.service.configs.AstConfigServiceConfig;
import ai.traceable.ast.config.service.store.AstFeatureConfigStore;
import ai.traceable.ast.config.service.store.ScanPurgeConfigStore;
import ai.traceable.ast.config.service.store.VulnerabilityMetadataOverridesStore;
import ai.traceable.ast.config.service.v1.AstEnabledConfig;
import ai.traceable.ast.config.service.v1.AstFeatureConfig;
import ai.traceable.ast.config.service.v1.AstFeatureConfigFilter;
import ai.traceable.ast.config.service.v1.AstFeatureConfigFilter.EnvironmentIdFilter;
import ai.traceable.ast.config.service.v1.AstReplayConfig;
import ai.traceable.ast.config.service.v1.CustomerDefinedTagsMap;
import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigRequest;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAstFeatureConfigsRequest;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.UpdateAstFeatureConfigRequest;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.VulnerabilityMetadataOverrides;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
class AstRulesManager implements RulesManager {
  private final ScanPurgeConfigStore scanPurgeConfigStore;
  private final VulnerabilityMetadataOverridesStore vulnerabilityMetadataOverridesStore;
  private final AstFeatureConfigStore astFeatureConfigStore;
  private final AstConfigServiceConfig config;

  @Override
  public ScanPurgeConfig updateScanPurgeConfig(
      RequestContext requestContext, UpdateScanPurgeConfigRequest request) {
    ScanPurgeConfig.Builder scanPurgeConfigBuilder =
        ScanPurgeConfig.newBuilder().setPurgeDuration(request.getPurgeConfig().getPurgeDuration());
    if (request.getPurgeConfig().hasScanRetentionLimitPerSuite()) {
      scanPurgeConfigBuilder.setScanRetentionLimitPerSuite(
          request.getPurgeConfig().getScanRetentionLimitPerSuite());
    }
    return scanPurgeConfigStore
        .upsertObject(requestContext, scanPurgeConfigBuilder.build())
        .getData();
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
  public List<VulnerabilityMetadataOverrides> getAllVulnerabilityMetadataOverridesConfig(
      RequestContext requestContext, GetAllVulnerabilityMetadataOverridesRequest request) {
    return vulnerabilityMetadataOverridesStore.getAllConfigData(requestContext);
  }

  @Override
  public List<AstFeatureConfig> getAstFeatureConfigs(
      RequestContext requestContext, GetAstFeatureConfigsRequest request) {
    if (request
        .getFilter()
        .getFilterCase()
        .equals(AstFeatureConfigFilter.FilterCase.FILTER_NOT_SET)) {
      return astFeatureConfigStore.getAllConfigData(requestContext);
    } else {
      return astFeatureConfigStore.getAllConfigData(requestContext, request.getFilter());
    }
  }

  @Override
  public AstFeatureConfig updateAstFeatureConfig(
      RequestContext requestContext, UpdateAstFeatureConfigRequest request) {
    AstFeatureConfig.Builder astFeatureConfigBuilder =
        AstFeatureConfig.newBuilder().setEnvironmentId(request.getEnvironmentId());
    switch (request.getUpdateStatusCase()) {
      case ENABLED_CONFIG:
        if (request.getEnabledConfig().hasReplayConfig()) {
          AstReplayConfig astReplayConfigFromRequest = request.getEnabledConfig().getReplayConfig();
          AstReplayConfig updatedAstReplayConfig =
              getUpdatedAstReplayConfig(
                  requestContext, astReplayConfigFromRequest, request.getEnvironmentId());
          astFeatureConfigBuilder.setEnabledConfig(
              AstEnabledConfig.newBuilder().setReplayConfig(updatedAstReplayConfig).build());
        } else {
          astFeatureConfigBuilder.setEnabledConfig(config.getDefaultAstEnabledConfig());
        }
        break;
      case DISABLED_CONFIG:
        astFeatureConfigBuilder.setDisabledConfig(request.getDisabledConfig());
        break;
    }
    astFeatureConfigStore.upsertObject(requestContext, astFeatureConfigBuilder.build());
    return astFeatureConfigBuilder.build();
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
    if (overriddenVulnerabilityMetadata.hasCustomerDefinedTags()) {
      existingVulnerabilityMetadata.clearCustomerDefinedTags();
      existingVulnerabilityMetadata.setCustomerDefinedTags(
          CustomerDefinedTagsMap.newBuilder()
              .putAllCustomerDefinedTags(
                  overriddenVulnerabilityMetadata
                      .getCustomerDefinedTags()
                      .getCustomerDefinedTagsMap())
              .build());
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
    if (overriddenVulnerabilityMetadata.hasEstimatedFixTime()) {
      existingVulnerabilityMetadata.setEstimatedFixTime(
          overriddenVulnerabilityMetadata.getEstimatedFixTime());
    }
  }

  private AstReplayConfig getUpdatedAstReplayConfig(
      RequestContext requestContext, AstReplayConfig requestConfig, String environmentId) {
    AstReplayConfig.Builder builder =
        AstReplayConfig.newBuilder()
            .setReplayEnabled(requestConfig.getReplayEnabled())
            .setSpanFilters(requestConfig.getSpanFilters());

    AstReplayConfig defaultConfig = config.getDefaultAstEnabledConfig().getReplayConfig();

    Optional<AstReplayConfig> currentConfig =
        this.getAstFeatureConfigs(
                requestContext,
                GetAstFeatureConfigsRequest.newBuilder()
                    .setFilter(
                        AstFeatureConfigFilter.newBuilder()
                            .setEnvironmentIdFilter(
                                EnvironmentIdFilter.newBuilder().addEnvironmentIds(environmentId))
                            .build())
                    .build())
            .stream()
            .findFirst()
            .map(AstFeatureConfig::getEnabledConfig)
            .map(AstEnabledConfig::getReplayConfig);

    if (requestConfig.hasApiInactivityDuration()) {
      builder.setApiInactivityDuration(requestConfig.getApiInactivityDuration());
    } else if (currentConfig.isPresent() && currentConfig.get().hasApiInactivityDuration()) {
      builder.setApiInactivityDuration(currentConfig.get().getApiInactivityDuration());
    } else {
      builder.setApiInactivityDuration(defaultConfig.getApiInactivityDuration());
    }

    if (requestConfig.hasMaxApiLimit()) {
      builder.setMaxApiLimit(requestConfig.getMaxApiLimit());
    } else if (currentConfig.isPresent() && currentConfig.get().hasMaxApiLimit()) {
      builder.setMaxApiLimit(currentConfig.get().getMaxApiLimit());
    } else {
      builder.setMaxApiLimit(defaultConfig.getMaxApiLimit());
    }

    if (requestConfig.hasSkipNonLearntApis()) {
      builder.setSkipNonLearntApis(requestConfig.getSkipNonLearntApis());
    } else if (currentConfig.isPresent() && currentConfig.get().hasSkipNonLearntApis()) {
      builder.setSkipNonLearntApis(currentConfig.get().getSkipNonLearntApis());
    } else {
      builder.setSkipNonLearntApis(defaultConfig.getSkipNonLearntApis());
    }

    if (requestConfig.hasSkipUnderDiscoveryApis()) {
      builder.setSkipUnderDiscoveryApis(requestConfig.getSkipUnderDiscoveryApis());
    } else if (currentConfig.isPresent() && currentConfig.get().hasSkipUnderDiscoveryApis()) {
      builder.setSkipUnderDiscoveryApis(currentConfig.get().getSkipUnderDiscoveryApis());
    } else {
      builder.setSkipUnderDiscoveryApis(defaultConfig.getSkipUnderDiscoveryApis());
    }

    return builder.build();
  }
}
