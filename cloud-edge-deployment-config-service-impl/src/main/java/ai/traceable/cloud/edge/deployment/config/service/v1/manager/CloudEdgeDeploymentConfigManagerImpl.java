package ai.traceable.cloud.edge.deployment.config.service.v1.manager;

import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.CreateCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeleteCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.SharedConfigMetadata;
import ai.traceable.cloud.edge.deployment.config.service.v1.UpdateCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.shared.config.SharedConfigMetadataRegistry;
import ai.traceable.cloud.edge.deployment.config.service.v1.store.CloudEdgeDeploymentConfigStore;
import ai.traceable.cloud.edge.deployment.config.service.v1.validator.CloudEdgeDeploymentValidator;
import ai.traceable.config.utils.UuidGenerator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CloudEdgeDeploymentConfigManagerImpl implements CloudEdgeDeploymentConfigManager {
  private final SharedConfigMetadataRegistry sharedConfigMetadataRegistry;

  private final CloudEdgeDeploymentConfigStore store;
  private final CloudEdgeDeploymentValidator validator;
  private final UuidGenerator uuidGenerator;

  @Override
  public CloudEdgeDeploymentConfig createCloudEdgeDeploymentConfig(
      RequestContext ctx, CreateCloudEdgeDeploymentConfigRequest request) {
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw new StatusRuntimeException(validationStatus);
    }

    // Generate a unique ID
    String id = uuidGenerator.generateRandomId();
    CloudEdgeDeploymentConfig config =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeploymentInputConfig(request.getCloudEdgeDeploymentInputConfig())
            .build();

    return store.upsertCloudEdgeDeploymentConfig(ctx, config, request.getConfigPermission());
  }

  @Override
  public List<CloudEdgeDeploymentConfig> getCloudEdgeDeploymentConfigs(
      RequestContext ctx, List<String> ids, ConfigAccessType accessType) {
    return store.getCloudEdgeDeploymentConfigs(ctx, ids, accessType);
  }

  @Override
  public CloudEdgeDeploymentConfig updateCloudEdgeDeploymentConfig(
      RequestContext ctx, String id, UpdateCloudEdgeDeploymentConfigRequest request) {
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw new StatusRuntimeException(validationStatus);
    }

    // Check if config with the given ID exists
    CloudEdgeDeploymentConfig existingConfig = store.getCloudEdgeDeploymentConfig(ctx, id);
    if (existingConfig == null) {
      throw new StatusRuntimeException(
          Status.NOT_FOUND.withDescription("Cloud edge deployment config not found"));
    }

    try {
      // Update config
      CloudEdgeDeploymentConfig.Builder updatedConfigBuilder = existingConfig.toBuilder();

      if (request.hasCloudEdgeDeploymentInputConfig()) {
        updatedConfigBuilder.setCloudEdgeDeploymentInputConfig(
            request.getCloudEdgeDeploymentInputConfig());
      }
      if (request.hasCloudEdgeDeployedOutputConfig()) {
        updatedConfigBuilder.setCloudEdgeDeployedOutputConfig(
            request.getCloudEdgeDeployedOutputConfig());
      }

      CloudEdgeDeploymentConfig updatedConfig = updatedConfigBuilder.build();

      return store.upsertCloudEdgeDeploymentConfig(
          ctx, updatedConfig, request.getConfigPermission());
    } catch (Exception e) {
      throw new StatusRuntimeException(
          Status.INTERNAL.withDescription(
              "Failed to update cloud edge deployment config: " + e.getMessage()));
    }
  }

  @Override
  public void deleteCloudEdgeDeploymentConfig(RequestContext ctx, String id) {
    DeleteCloudEdgeDeploymentConfigRequest request =
        DeleteCloudEdgeDeploymentConfigRequest.newBuilder().setId(id).build();
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }

    // Check if config exists and delete it
    CloudEdgeDeploymentConfig config = store.getCloudEdgeDeploymentConfig(ctx, id);
    if (config == null) {
      throw Status.NOT_FOUND
          .withDescription("Cloud edge deployment config not found with id: " + id)
          .asRuntimeException();
    }

    try {
      store.deleteCloudEdgeDeploymentConfig(ctx, id);
    } catch (Exception e) {
      throw new StatusRuntimeException(
          Status.INTERNAL.withDescription(
              "Failed to delete cloud edge deployment config: " + e.getMessage()));
    }
  }

  @Override
  public SharedConfigMetadata getSharedConfigMetadata(
      RequestContext ctx, ConfigAccessType accessType) {
    return sharedConfigMetadataRegistry.getSharedConfigMetadataWithReadPermission(accessType);
  }
}
