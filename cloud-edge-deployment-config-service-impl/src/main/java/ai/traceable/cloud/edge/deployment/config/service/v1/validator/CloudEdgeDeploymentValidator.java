package ai.traceable.cloud.edge.deployment.config.service.v1.validator;

import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentInputConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigPermission;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigValueDescriptor;
import ai.traceable.cloud.edge.deployment.config.service.v1.CreateCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeleteCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.ServiceConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.SharedConfigMetadata;
import ai.traceable.cloud.edge.deployment.config.service.v1.UpdateCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.shared.config.SharedConfigMetadataRegistry;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CloudEdgeDeploymentValidator {
  private final SharedConfigMetadataRegistry sharedConfigMetadataRegistry;

  public Status validate(CreateCloudEdgeDeploymentConfigRequest request) {
    if (!request.hasCloudEdgeDeploymentInputConfig()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Cloud edge deployment input config is required");
    }

    if (!request.hasConfigPermission()) {
      return Status.INVALID_ARGUMENT.withDescription("Config permission is required");
    }
    Status status =
        validateInputConfig(
            request.getCloudEdgeDeploymentInputConfig(), request.getConfigPermission());
    if (!status.equals(Status.OK)) {
      return status;
    }

    return Status.OK;
  }

  public Status validate(UpdateCloudEdgeDeploymentConfigRequest request) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Cloud edge deployment config ID cannot be empty");
    }

    if (!request.hasConfigPermission()) {
      return Status.INVALID_ARGUMENT.withDescription("Config permission is required");
    }

    if (request.getConfigPermission().getWrite().equals(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
        && request.hasCloudEdgeDeployedOutputConfig()) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "User with permission %s can't update out config", request.getConfigPermission()));
    }
    if (request.hasCloudEdgeDeploymentInputConfig()) {
      Status status =
          validateInputConfig(
              request.getCloudEdgeDeploymentInputConfig(), request.getConfigPermission());
      if (!status.equals(Status.OK)) {
        return status;
      }
    }

    return Status.OK;
  }

  public Status validate(DeleteCloudEdgeDeploymentConfigRequest request) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Cloud edge deployment config ID cannot be empty");
    }
    return Status.OK;
  }

  private Status validateInputConfig(
      CloudEdgeDeploymentInputConfig cloudEdgeDeploymentInputConfig,
      ConfigPermission configPermission) {

    Set<String> serviceNames = new HashSet<>();
    for (ServiceConfig serviceConfig : cloudEdgeDeploymentInputConfig.getServiceConfigsList()) {
      String serviceName = serviceConfig.getServiceName();
      if (!serviceNames.add(serviceName)) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "Duplicate service name found: %s. Service names must be unique.", serviceName));
      }
    }

    SharedConfigMetadata sharedConfigMetadata =
        sharedConfigMetadataRegistry.getSharedConfigMetadataWithWritePermission(
            configPermission.getWrite());
    Map<String, ConfigValueDescriptor> clusterConfigDetailMap =
        sharedConfigMetadata.getClusterConfigDetailsMap();
    Map<String, ConfigValueDescriptor> serviceConfigDetailMap =
        sharedConfigMetadata.getServiceConfigDetailsMap();
    for (String key :
        cloudEdgeDeploymentInputConfig
            .getClusterConfig()
            .getAdvancedConfig()
            .getGenericConfig()
            .getFieldsMap()
            .keySet()) {
      if (!clusterConfigDetailMap.containsKey(key)) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "User with permission %s can't update cluster advanced config key %s",
                configPermission, key));
      }
    }
    for (ServiceConfig serviceConfig : cloudEdgeDeploymentInputConfig.getServiceConfigsList()) {
      for (String key :
          serviceConfig.getAdvancedConfig().getGenericConfig().getFieldsMap().keySet()) {
        if (!serviceConfigDetailMap.containsKey(key)) {
          return Status.INVALID_ARGUMENT.withDescription(
              String.format(
                  "User with permission %s can't update service advanced config key %s and service ",
                  configPermission, key));
        }
      }
    }
    return Status.OK;
  }
}
