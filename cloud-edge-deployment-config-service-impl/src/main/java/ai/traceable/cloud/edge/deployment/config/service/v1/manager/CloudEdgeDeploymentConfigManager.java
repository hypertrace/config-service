package ai.traceable.cloud.edge.deployment.config.service.v1.manager;

import ai.traceable.cloud.edge.deployment.config.service.v1.*;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface CloudEdgeDeploymentConfigManager {

  CloudEdgeDeploymentConfig createCloudEdgeDeploymentConfig(
      RequestContext ctx, CreateCloudEdgeDeploymentConfigRequest request);

  List<CloudEdgeDeploymentConfig> getCloudEdgeDeploymentConfigs(
      RequestContext ctx, GetCloudEdgeDeploymentConfigsFilter filter, ConfigAccessType accessType);

  CloudEdgeDeploymentConfig updateCloudEdgeDeploymentConfig(
      RequestContext ctx, String id, UpdateCloudEdgeDeploymentConfigRequest request);

  void deleteCloudEdgeDeploymentConfig(RequestContext ctx, String id);

  void cancelCloudEdgeDeploymentConfigAction(
      RequestContext ctx, CancelCloudEdgeDeploymentConfigActionRequest request);

  SharedConfigMetadata getSharedConfigMetadata(RequestContext ctx, ConfigAccessType accessType);

  void removeCloudEdgeDeploymentConfig(
      RequestContext ctx, RemoveCloudEdgeDeploymentConfigRequest request);

  void holdCloudEdgeDeploymentConfig(
      RequestContext ctx, HoldCloudEdgeDeploymentConfigRequest request);

  void deployCloudEdgeDeploymentConfig(
      RequestContext ctx, DeployCloudEdgeDeploymentConfigRequest request);
}
