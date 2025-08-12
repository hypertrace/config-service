package ai.traceable.cloud.edge.deployment.config.service.v1.manager;

import ai.traceable.cloud.edge.deployment.config.service.v1.Action;
import ai.traceable.cloud.edge.deployment.config.service.v1.CancelCloudEdgeDeploymentConfigActionRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfigActionRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfigWithActions;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentOutputConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.CreateCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeleteCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeployCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeploymentStatus;
import ai.traceable.cloud.edge.deployment.config.service.v1.GetCloudEdgeDeploymentConfigsFilter;
import ai.traceable.cloud.edge.deployment.config.service.v1.GetCloudEdgeDeploymentConfigsResponse;
import ai.traceable.cloud.edge.deployment.config.service.v1.HoldCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.RemoveCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.SharedConfigMetadata;
import ai.traceable.cloud.edge.deployment.config.service.v1.UpdateCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.shared.config.SharedConfigMetadataRegistry;
import ai.traceable.cloud.edge.deployment.config.service.v1.state.transitions.StateTransitionsRegistry;
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
  private final StateTransitionsRegistry stateTransitionsRegistry;

  private static final List<DeploymentStatus> notifiableDeploymentStatuses =
      List.of(
          DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED,
          DeploymentStatus.DEPLOYMENT_STATUS_CHANGE_REQUESTED,
          DeploymentStatus.DEPLOYMENT_STATUS_REMOVAL_REQUESTED);

  @Override
  public CloudEdgeDeploymentConfig createCloudEdgeDeploymentConfig(
      RequestContext ctx, CreateCloudEdgeDeploymentConfigRequest request) {
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw new StatusRuntimeException(validationStatus);
    }

    DeploymentStatus updatedStatus =
        validator
            .validateActionAndGetNextStates(
                null, request.getConfigPermission().getWrite(), Action.ACTION_CREATE)
            .get(0);

    // Generate a unique ID
    String id = uuidGenerator.generateRandomId();
    CloudEdgeDeploymentConfig config =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeploymentInputConfig(request.getCloudEdgeDeploymentInputConfig())
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder().setStatus(updatedStatus).build())
            .build();

    if (notifiableDeploymentStatuses.contains(updatedStatus)) {
      return getTimer(ctx.getTenantId().orElseThrow(), id, updatedStatus.name())
          .record(
              () ->
                  store.upsertCloudEdgeDeploymentConfig(
                      ctx, config, request.getConfigPermission()));
    }

    return store.upsertCloudEdgeDeploymentConfig(ctx, config, request.getConfigPermission());
  }

  public List<CloudEdgeDeploymentConfig> getCloudEdgeDeploymentConfigs(
      RequestContext ctx, GetCloudEdgeDeploymentConfigsFilter filter, ConfigAccessType accessType) {
    return store.getCloudEdgeDeploymentConfigs(ctx, filter, accessType);
  }

  public GetCloudEdgeDeploymentConfigsResponse getCloudEdgeDeploymentConfigsWithActions(
      RequestContext ctx, GetCloudEdgeDeploymentConfigsFilter filter, ConfigAccessType accessType) {

    List<CloudEdgeDeploymentConfig> configs =
        getCloudEdgeDeploymentConfigs(ctx, filter, accessType);
    List<CloudEdgeDeploymentConfigWithActions> configsWithActions =
        configs.stream()
            .map(
                config -> {
                  var status = config.getCloudEdgeDeployedOutputConfig().getStatus();
                  var actionsMap = stateTransitionsRegistry.getActionsMap(status, accessType);

                  var builder =
                      CloudEdgeDeploymentConfigWithActions.newBuilder()
                          .setCloudEdgeDeploymentConfig(config);

                  actionsMap.forEach(
                      (action, states) -> {
                        builder.addAllowedActions(action);
                        if (action == Action.ACTION_UPDATE_STATUS) {
                          builder.addAllAllowedStatuses(states);
                        }
                      });
                  builder.addAllowedActions(Action.ACTION_VIEW);
                  return builder.build();
                })
            .collect(Collectors.toList());

    return GetCloudEdgeDeploymentConfigsResponse.newBuilder()
        .addAllCloudEdgeDeployments(configs)
        .addAllCloudEdgeDeploymentsWithActions(configsWithActions)
        .build();
  }

  @Override
  public CloudEdgeDeploymentConfig updateCloudEdgeDeploymentConfig(
      RequestContext ctx, String id, UpdateCloudEdgeDeploymentConfigRequest request) {
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw new StatusRuntimeException(validationStatus);
    }

    // Check if config with the given ID exists
    CloudEdgeDeploymentConfig existingConfig = getExistingConfigOrThrow(ctx, id);
    DeploymentStatus currentStatus = existingConfig.getCloudEdgeDeployedOutputConfig().getStatus();
    DeploymentStatus updatedStatus = null;

    // Update config
    CloudEdgeDeploymentConfig.Builder updatedConfigBuilder = existingConfig.toBuilder();

    if (request.hasCloudEdgeDeploymentInputConfig()) {
      Action action = Action.ACTION_EDIT;
      updatedStatus =
          validator
              .validateActionAndGetNextStates(
                  currentStatus, request.getConfigPermission().getWrite(), action)
              .get(0);
      updatedConfigBuilder.setCloudEdgeDeploymentInputConfig(
          request.getCloudEdgeDeploymentInputConfig());
      updatedConfigBuilder.getCloudEdgeDeployedOutputConfigBuilder().setStatus(updatedStatus);
    } else if (request.hasCloudEdgeDeployedOutputConfig()) {
      Action action = Action.ACTION_UPDATE_STATUS;
      validator.validateUpdatedStatus(
          currentStatus,
          request.getConfigPermission().getWrite(),
          action,
          request.getCloudEdgeDeployedOutputConfig().getStatus());
      updatedStatus = request.getCloudEdgeDeployedOutputConfig().getStatus();
      updatedConfigBuilder.setCloudEdgeDeployedOutputConfig(
          request.getCloudEdgeDeployedOutputConfig());
    }

    CloudEdgeDeploymentConfig updatedConfig = updatedConfigBuilder.build();

    try {
      if (!notifiableDeploymentStatuses.contains(currentStatus)
          && notifiableDeploymentStatuses.contains(updatedStatus)) {
        return getTimer(ctx.getTenantId().orElseThrow(), id, updatedStatus.name())
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
    CloudEdgeDeploymentConfig config = getExistingConfigOrThrow(ctx, id);
    validator.validateActionAndGetNextStates(
        config.getCloudEdgeDeployedOutputConfig().getStatus(),
        ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE,
        Action.ACTION_DELETE);

    try {
      store.deleteCloudEdgeDeploymentConfig(ctx, id);
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
    CloudEdgeDeploymentConfig existingConfig = getExistingConfigOrThrow(ctx, request.getId());
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
    CloudEdgeDeploymentConfig existingConfig = getExistingConfigOrThrow(ctx, request.getId());
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

  @Override
  public void holdCloudEdgeDeploymentConfig(
      RequestContext ctx, HoldCloudEdgeDeploymentConfigRequest request) {
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }

    // Check if config exists
    CloudEdgeDeploymentConfig existingConfig = getExistingConfigOrThrow(ctx, request.getId());
    DeploymentStatus updatedStatus =
        validator
            .validateActionAndGetNextStates(
                existingConfig.getCloudEdgeDeployedOutputConfig().getStatus(),
                request.getAccessType(),
                Action.ACTION_HOLD)
            .get(0);

    CloudEdgeDeploymentConfig.Builder updatedConfigBuilder = existingConfig.toBuilder();
    updatedConfigBuilder.getCloudEdgeDeployedOutputConfigBuilder().setStatus(updatedStatus).build();

    CloudEdgeDeploymentConfig updatedConfig = updatedConfigBuilder.build();
    store.upsertCloudEdgeDeploymentConfig(ctx, updatedConfig);
  }

  @Override
  public void deployCloudEdgeDeploymentConfig(
      RequestContext ctx, DeployCloudEdgeDeploymentConfigRequest request) {
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }

    // Check if config exists
    CloudEdgeDeploymentConfig existingConfig = getExistingConfigOrThrow(ctx, request.getId());
    DeploymentStatus updatedStatus =
        validator
            .validateActionAndGetNextStates(
                existingConfig.getCloudEdgeDeployedOutputConfig().getStatus(),
                request.getAccessType(),
                Action.ACTION_DEPLOY)
            .get(0);

    CloudEdgeDeploymentConfig.Builder updatedConfigBuilder = existingConfig.toBuilder();
    updatedConfigBuilder.getCloudEdgeDeployedOutputConfigBuilder().setStatus(updatedStatus).build();

    CloudEdgeDeploymentConfig updatedConfig = updatedConfigBuilder.build();
    store.upsertCloudEdgeDeploymentConfig(ctx, updatedConfig);
  }

  @Override
  public void performCloudEdgeDeploymentConfigAction(
      RequestContext ctx, CloudEdgeDeploymentConfigActionRequest request) {
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }

    // Check if config exists
    CloudEdgeDeploymentConfig existingConfig = getExistingConfigOrThrow(ctx, request.getId());
    DeploymentStatus updatedStatus =
        validator
            .validateActionAndGetNextStates(
                existingConfig.getCloudEdgeDeployedOutputConfig().getStatus(),
                request.getAccessType(),
                request.getAction())
            .get(0);

    CloudEdgeDeploymentConfig.Builder updatedConfigBuilder = existingConfig.toBuilder();
    updatedConfigBuilder.getCloudEdgeDeployedOutputConfigBuilder().setStatus(updatedStatus);

    if (request.getAction() == Action.ACTION_CANCEL_CHANGE_REQUEST) {
      updatedConfigBuilder.setCloudEdgeDeploymentInputConfig(
          existingConfig.getLastAppliedInputConfig());
    }

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

  private CloudEdgeDeploymentConfig getExistingConfigOrThrow(RequestContext ctx, String id) {
    CloudEdgeDeploymentConfig existingConfig = store.getCloudEdgeDeploymentConfig(ctx, id);
    if (existingConfig == null) {
      throw Status.NOT_FOUND
          .withDescription("Cloud edge deployment config not found with id: " + id)
          .asRuntimeException();
    }
    return existingConfig;
  }
}
