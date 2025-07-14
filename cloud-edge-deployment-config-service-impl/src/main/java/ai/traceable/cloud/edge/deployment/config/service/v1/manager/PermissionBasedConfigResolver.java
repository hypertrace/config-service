package ai.traceable.cloud.edge.deployment.config.service.v1.manager;

import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeploymentStatus;
import ai.traceable.cloud.edge.deployment.config.service.v1.ServiceConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.SharedConfigMetadata;
import ai.traceable.cloud.edge.deployment.config.service.v1.shared.config.SharedConfigMetadataRegistry;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
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
      if (newConfig.hasCloudEdgeDeploymentInputConfig()) {
        updatedConfigBuilder.setCloudEdgeDeploymentInputConfig(
            newConfig.getCloudEdgeDeploymentInputConfig());
      }
      if (newConfig.hasCloudEdgeDeployedOutputConfig()) {
        updatedConfigBuilder.setCloudEdgeDeployedOutputConfig(
            newConfig.getCloudEdgeDeployedOutputConfig());
      }
      return updatedConfigBuilder.build();
    }

    updatedConfigBuilder
        .getCloudEdgeDeploymentInputConfigBuilder()
        .mergeFrom(newConfig.getCloudEdgeDeploymentInputConfig());
    updatedConfigBuilder
        .getCloudEdgeDeployedOutputConfigBuilder()
        .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS);
    return updatedConfigBuilder.build();
  }
}
