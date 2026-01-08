package ai.traceable.cloud.edge.deployment.config.service.v1.manager;

import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentInputConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ClusterAdvancedConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ClusterConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeploymentStatus;
import ai.traceable.cloud.edge.deployment.config.service.v1.ServiceAdvancedConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ServiceConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.SharedConfigMetadata;
import ai.traceable.cloud.edge.deployment.config.service.v1.shared.config.SharedConfigMetadataRegistry;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class PermissionBasedConfigResolver {
  private final SharedConfigMetadataRegistry sharedConfigMetadataRegistry;

  public CloudEdgeDeploymentConfig getResolvedConfigForReadRequest(
      CloudEdgeDeploymentConfig config, ConfigAccessType accessType) {
    SharedConfigMetadata sharedConfigMetadata =
        sharedConfigMetadataRegistry.getSharedConfigMetadataWithReadPermission(accessType);
    CloudEdgeDeploymentConfig.Builder builder = config.toBuilder();

    Map<String, Value> filteredClusterAdvancedConfig =
        config
            .getCloudEdgeDeploymentInputConfig()
            .getClusterConfig()
            .getAdvancedConfig()
            .getGenericConfig()
            .getFieldsMap()
            .entrySet()
            .stream()
            .filter(
                entry ->
                    sharedConfigMetadata.getClusterConfigDetailsMap().containsKey(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    builder
        .getCloudEdgeDeploymentInputConfigBuilder()
        .getClusterConfigBuilder()
        .getAdvancedConfigBuilder()
        .setGenericConfig(Struct.newBuilder().putAllFields(filteredClusterAdvancedConfig).build());

    List<ServiceConfig> serviceConfigList =
        config.getCloudEdgeDeploymentInputConfig().getServiceConfigsList().stream()
            .map(
                serviceConfig -> {
                  ServiceConfig.Builder serviceConfigBuilder = serviceConfig.toBuilder();
                  Map<String, Value> filteredServiceAdvancedConfig =
                      serviceConfig
                          .getAdvancedConfig()
                          .getGenericConfig()
                          .getFieldsMap()
                          .entrySet()
                          .stream()
                          .filter(
                              entry ->
                                  sharedConfigMetadata
                                      .getServiceConfigDetailsMap()
                                      .containsKey(entry.getKey()))
                          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                  serviceConfigBuilder
                      .getAdvancedConfigBuilder()
                      .setGenericConfig(
                          Struct.newBuilder().putAllFields(filteredServiceAdvancedConfig).build());
                  return serviceConfigBuilder.build();
                })
            .collect(Collectors.toUnmodifiableList());

    builder
        .getCloudEdgeDeploymentInputConfigBuilder()
        .clearServiceConfigs()
        .addAllServiceConfigs(serviceConfigList);
    return builder.build();
  }

  public CloudEdgeDeploymentConfig getResolvedConfigForWriteRequest(
      CloudEdgeDeploymentConfig newConfig,
      CloudEdgeDeploymentConfig existingConfig,
      ConfigAccessType accessType) {

    CloudEdgeDeploymentConfig.Builder updatedConfigBuilder = existingConfig.toBuilder();

    if (accessType.equals(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)) {
      return applyFullOverride(updatedConfigBuilder, newConfig);
    }

    return applyAdvanceConfigMerge(updatedConfigBuilder, newConfig);
  }

  private CloudEdgeDeploymentConfig applyFullOverride(
      CloudEdgeDeploymentConfig.Builder updatedConfigBuilder, CloudEdgeDeploymentConfig newConfig) {

    if (newConfig.hasCloudEdgeDeploymentInputConfig()) {
      updatedConfigBuilder.setCloudEdgeDeploymentInputConfig(
          newConfig.getCloudEdgeDeploymentInputConfig());
    }

    if (newConfig.hasCloudEdgeDeployedOutputConfig()) {
      updatedConfigBuilder.setCloudEdgeDeployedOutputConfig(
          newConfig.getCloudEdgeDeployedOutputConfig());

      if (DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY.equals(
          newConfig.getCloudEdgeDeployedOutputConfig().getStatus())) {
        updatedConfigBuilder.setLastAppliedInputConfig(
            updatedConfigBuilder.getCloudEdgeDeploymentInputConfig());
      }
    }

    return updatedConfigBuilder.build();
  }

  private CloudEdgeDeploymentConfig applyAdvanceConfigMerge(
      CloudEdgeDeploymentConfig.Builder updatedConfigBuilder, CloudEdgeDeploymentConfig newConfig) {

    CloudEdgeDeploymentInputConfig.Builder existingInputBuilder =
        updatedConfigBuilder.getCloudEdgeDeploymentInputConfigBuilder();

    ClusterConfig mergedClusterConfig =
        buildMergedClusterConfig(
            existingInputBuilder.getClusterConfig(),
            newConfig.getCloudEdgeDeploymentInputConfig().getClusterConfig());
    existingInputBuilder.setClusterConfig(mergedClusterConfig);

    List<ServiceConfig> mergedServices =
        buildMergedServiceConfigs(
            existingInputBuilder.getServiceConfigsList(),
            newConfig.getCloudEdgeDeploymentInputConfig().getServiceConfigsList());
    existingInputBuilder.clearServiceConfigs().addAllServiceConfigs(mergedServices);

    updatedConfigBuilder
        .getCloudEdgeDeployedOutputConfigBuilder()
        .setStatus(newConfig.getCloudEdgeDeployedOutputConfig().getStatus());

    return updatedConfigBuilder.build();
  }

  private ClusterConfig buildMergedClusterConfig(
      ClusterConfig existingClusterConfig, ClusterConfig newClusterConfig) {

    Struct mergedGenericConfig =
        existingClusterConfig.getAdvancedConfig().getGenericConfig().toBuilder()
            .mergeFrom(newClusterConfig.getAdvancedConfig().getGenericConfig())
            .build();
    return ClusterConfig.newBuilder()
        .setClusterName(newClusterConfig.getClusterName())
        .setEnvironmentName(newClusterConfig.getEnvironmentName())
        .addAllPrimaryRegions(newClusterConfig.getPrimaryRegionsList())
        .addAllSecondaryRegions(newClusterConfig.getSecondaryRegionsList())
        .setAdvancedConfig(
            ClusterAdvancedConfig.newBuilder().setGenericConfig(mergedGenericConfig).build())
        .build();
  }

  private List<ServiceConfig> buildMergedServiceConfigs(
      List<ServiceConfig> existingServiceConfigs, List<ServiceConfig> newServiceConfigs) {

    Map<String, ServiceConfig> existingMap =
        existingServiceConfigs.stream()
            .collect(Collectors.toMap(ServiceConfig::getServiceName, s -> s));

    List<ServiceConfig> result = new ArrayList<>();
    for (ServiceConfig newService : newServiceConfigs) {
      ServiceConfig existing = existingMap.get(newService.getServiceName());
      if (existing != null) {
        result.add(mergeServiceConfigs(existing, newService));
      } else {
        result.add(newService); // New service, full override
      }
    }
    return result;
  }

  private ServiceConfig mergeServiceConfigs(ServiceConfig existing, ServiceConfig incoming) {
    Struct mergedGenericConfig =
        existing.getAdvancedConfig().getGenericConfig().toBuilder()
            .mergeFrom(incoming.getAdvancedConfig().getGenericConfig())
            .build();
    ServiceConfig.Builder builder =
        ServiceConfig.newBuilder()
            .setServiceName(incoming.getServiceName())
            .addAllDomainConfigs(incoming.getDomainConfigsList())
            .addAllOriginConfigs(incoming.getOriginConfigsList())
            .setAdvancedConfig(
                ServiceAdvancedConfig.newBuilder().setGenericConfig(mergedGenericConfig).build());

    if (incoming.hasHealthCheckDetails()) {
      builder.setHealthCheckDetails(incoming.getHealthCheckDetails());
    }

    return builder.build();
  }
}
