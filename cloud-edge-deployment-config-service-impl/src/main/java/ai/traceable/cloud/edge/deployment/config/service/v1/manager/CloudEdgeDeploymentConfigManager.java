package ai.traceable.cloud.edge.deployment.config.service.v1.manager;

import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfigActionRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.CreateCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeleteCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.GetCloudEdgeDeploymentConfigsFilter;
import ai.traceable.cloud.edge.deployment.config.service.v1.GetCloudEdgeDeploymentConfigsResponse;
import ai.traceable.cloud.edge.deployment.config.service.v1.SharedConfigMetadata;
import ai.traceable.cloud.edge.deployment.config.service.v1.UpdateCloudEdgeDeploymentConfigRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface CloudEdgeDeploymentConfigManager {

  CloudEdgeDeploymentConfig createCloudEdgeDeploymentConfig(
      RequestContext ctx, CreateCloudEdgeDeploymentConfigRequest request);

  List<CloudEdgeDeploymentConfig> getCloudEdgeDeploymentConfigs(
      RequestContext ctx, GetCloudEdgeDeploymentConfigsFilter filter, ConfigAccessType accessType);

  GetCloudEdgeDeploymentConfigsResponse getCloudEdgeDeploymentConfigsWithActions(
      RequestContext ctx, GetCloudEdgeDeploymentConfigsFilter filter, ConfigAccessType accessType);

  CloudEdgeDeploymentConfig updateCloudEdgeDeploymentConfig(
      RequestContext ctx, String id, UpdateCloudEdgeDeploymentConfigRequest request);

  void deleteCloudEdgeDeploymentConfig(
      RequestContext ctx, DeleteCloudEdgeDeploymentConfigRequest request);

  SharedConfigMetadata getSharedConfigMetadata(RequestContext ctx, ConfigAccessType accessType);

  void performCloudEdgeDeploymentConfigAction(
      RequestContext ctx, CloudEdgeDeploymentConfigActionRequest request);
}
