package ai.traceable.cloud.edge.deployment.config.service.v1.validator;

import ai.traceable.cloud.edge.deployment.config.service.v1.*;
import io.grpc.Status;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CloudEdgeDeploymentValidator {

  public Status validate(CreateCloudEdgeDeploymentConfigRequest request) {
    if (!request.hasCloudEdgeDeploymentInputConfig()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Cloud edge deployment input config is required");
    }

    if (!request.hasConfigPermission()) {
      return Status.INVALID_ARGUMENT.withDescription("Config permission is required");
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

    return Status.OK;
  }

  public Status validate(DeleteCloudEdgeDeploymentConfigRequest request) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Cloud edge deployment config ID cannot be empty");
    }
    return Status.OK;
  }
}
