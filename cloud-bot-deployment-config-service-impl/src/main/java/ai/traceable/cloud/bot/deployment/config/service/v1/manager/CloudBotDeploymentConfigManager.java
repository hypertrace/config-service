package ai.traceable.cloud.bot.deployment.config.service.v1.manager;

import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CreateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.EnableCloudBotDeploymentRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.GetCloudBotDeploymentConfigsRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentStatusRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface CloudBotDeploymentConfigManager {

  CloudBotDeploymentConfig createCloudBotDeploymentConfig(
      RequestContext ctx, CreateCloudBotDeploymentConfigRequest request);

  CloudBotDeploymentConfig updateCloudBotDeploymentConfig(
      RequestContext ctx, String id, UpdateCloudBotDeploymentConfigRequest request);

  CloudBotDeploymentConfig updateCloudBotDeploymentStatus(
      RequestContext ctx, String id, UpdateCloudBotDeploymentStatusRequest request);

  void deleteCloudBotDeploymentConfig(RequestContext ctx, String id);

  void enableCloudBotDeployment(RequestContext ctx, EnableCloudBotDeploymentRequest request);

  CloudBotDeploymentConfig rotateApiToken(RequestContext ctx, String id);

  List<CloudBotDeploymentConfig> getCloudBotDeploymentConfigs(
      RequestContext ctx, GetCloudBotDeploymentConfigsRequest request);
}
