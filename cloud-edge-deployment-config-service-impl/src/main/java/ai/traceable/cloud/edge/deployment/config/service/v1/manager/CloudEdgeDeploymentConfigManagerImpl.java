package ai.traceable.cloud.edge.deployment.config.service.v1.manager;

import ai.traceable.cloud.edge.deployment.config.service.v1.Action;
import ai.traceable.cloud.edge.deployment.config.service.v1.CancelCloudEdgeDeploymentConfigActionRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.CreateCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeleteCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeploymentStatus;
import ai.traceable.cloud.edge.deployment.config.service.v1.GetCloudEdgeDeploymentConfigsFilter;
import ai.traceable.cloud.edge.deployment.config.service.v1.RemoveCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.SharedConfigMetadata;
import ai.traceable.cloud.edge.deployment.config.service.v1.UpdateCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.shared.config.SharedConfigMetadataRegistry;
import ai.traceable.cloud.edge.deployment.config.service.v1.store.CloudEdgeDeploymentConfigStore;
import ai.traceable.cloud.edge.deployment.config.service.v1.validator.CloudEdgeDeploymentValidator;
import ai.traceable.config.utils.UuidGenerator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CloudEdgeDeploymentConfigManagerImpl implements CloudEdgeDeploymentConfigManager {
  private final SharedConfigMetadataRegistry sharedConfigMetadataRegistry;

  private static final String TENANT_ID_TAG = "tenantId";
  private static final String CONFIG_ID_TAG = "deploymentId";
  private static final String DEPLOYMENT_STATUS_TAG = "deploymentStatus";
  private static final String CLOUD_EDGE_DEPLOYMENT_STATUS_ACTION_TIMER =
      "cloud.edge.deployment.status.action.timer";
  private static final Map<Tags, Timer> TIMER_MAP = new ConcurrentHashMap<>();

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

    return getTimer(
            ctx.getTenantId().orElseThrow(),
            id,
            DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS.name())
        .record(
            () ->
                store.upsertCloudEdgeDeploymentConfig(ctx, config, request.getConfigPermission()));
  }

  public List<CloudEdgeDeploymentConfig> getCloudEdgeDeploymentConfigs(
      RequestContext ctx, GetCloudEdgeDeploymentConfigsFilter filter, ConfigAccessType accessType) {
    return store.getCloudEdgeDeploymentConfigs(ctx, filter, accessType);
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

      if (existingConfig.hasCloudEdgeDeployedOutputConfig()
          && !DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS.equals(
              existingConfig.getCloudEdgeDeployedOutputConfig().getStatus())) {
        return getTimer(
                ctx.getTenantId().orElseThrow(),
                id,
                DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS.name())
            .record(
                () ->
                    store.upsertCloudEdgeDeploymentConfig(
                        ctx, updatedConfig, request.getConfigPermission()));
      }

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
      getTimer(ctx.getTenantId().orElseThrow(), id, "DEPLOYMENT_DELETED")
          .record(() -> store.deleteCloudEdgeDeploymentConfig(ctx, id));
    } catch (Exception e) {
      throw new StatusRuntimeException(
          Status.INTERNAL.withDescription(
              "Failed to delete cloud edge deployment config: " + e.getMessage()));
    }
  }

  @Override
  public void cancelCloudEdgeDeploymentConfigAction(
      RequestContext ctx, CancelCloudEdgeDeploymentConfigActionRequest request) {
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }

    // Check if config exists
    CloudEdgeDeploymentConfig existingConfig =
        store.getCloudEdgeDeploymentConfig(ctx, request.getId());
    if (existingConfig == null) {
      throw Status.NOT_FOUND
          .withDescription("Cloud edge deployment config not found with id: " + request.getId())
          .asRuntimeException();
    }

    DeploymentStatus currentStatus = existingConfig.getCloudEdgeDeployedOutputConfig().getStatus();
    Action action =
        DeploymentStatus.DEPLOYMENT_STATUS_CHANGE_REQUESTED.equals(currentStatus)
            ? Action.ACTION_CANCEL_CHANGE_REQUEST
            : Action.ACTION_CANCEL_REMOVAL_REQUEST;

    DeploymentStatus updatedStatus =
        validator
            .validateActionAndGetNextStates(currentStatus, request.getAccessType(), action)
            .get(0);

    CloudEdgeDeploymentConfig.Builder updatedConfigBuilder = existingConfig.toBuilder();
    updatedConfigBuilder.getCloudEdgeDeployedOutputConfigBuilder().setStatus(updatedStatus).build();

    CloudEdgeDeploymentConfig updatedConfig = updatedConfigBuilder.build();
    store.upsertCloudEdgeDeploymentConfig(ctx, updatedConfig);
  }

  @Override
  public SharedConfigMetadata getSharedConfigMetadata(
      RequestContext ctx, ConfigAccessType accessType) {
    return sharedConfigMetadataRegistry.getSharedConfigMetadataWithReadPermission(accessType);
  }

  @Override
  public void removeCloudEdgeDeploymentConfig(
      RequestContext ctx, RemoveCloudEdgeDeploymentConfigRequest request) {
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }

    // Check if config exists
    CloudEdgeDeploymentConfig existingConfig =
        store.getCloudEdgeDeploymentConfig(ctx, request.getId());
    if (existingConfig == null) {
      throw Status.NOT_FOUND
          .withDescription("Cloud edge deployment config not found with id: " + request.getId())
          .asRuntimeException();
    }

    DeploymentStatus updatedStatus =
        validator
            .validateActionAndGetNextStates(
                existingConfig.getCloudEdgeDeployedOutputConfig().getStatus(),
                request.getAccessType(),
                Action.ACTION_REQUEST_REMOVAL)
            .get(0);

    CloudEdgeDeploymentConfig.Builder updatedConfigBuilder = existingConfig.toBuilder();
    updatedConfigBuilder.getCloudEdgeDeployedOutputConfigBuilder().setStatus(updatedStatus).build();

    CloudEdgeDeploymentConfig updatedConfig = updatedConfigBuilder.build();
    store.upsertCloudEdgeDeploymentConfig(ctx, updatedConfig);
  }

  private Timer getTimer(String tenantId, String configId, String deploymentStatus) {
    Tags metricTags =
        Tags.of(
            TENANT_ID_TAG,
            tenantId,
            DEPLOYMENT_STATUS_TAG,
            deploymentStatus,
            CONFIG_ID_TAG,
            configId);

    return TIMER_MAP.computeIfAbsent(
        metricTags,
        id ->
            PlatformMetricsRegistry.registerTimer(
                CLOUD_EDGE_DEPLOYMENT_STATUS_ACTION_TIMER,
                metricTags.stream().collect(Collectors.toMap(Tag::getKey, Tag::getValue))));
  }
}
