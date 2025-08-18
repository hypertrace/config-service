package ai.traceable.cloud.edge.deployment.config.service.v1.validator;

import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigServiceGrpc.CloudBotDeploymentConfigServiceBlockingStub;
import ai.traceable.cloud.bot.deployment.config.service.v1.GetCloudBotDeploymentConfigsRequest;
import io.grpc.Status;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

/** Validator for checking cloud edge deployment usage in bot configurations. */
@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
@Singleton
public class EdgeDeploymentUsageValidator {
  private final CloudBotDeploymentConfigServiceBlockingStub cloudBotDeploymentConfigService;

  /**
   * Validates if a cloud edge deployment is not in use by any bot configuration.
   *
   * @param ctx The request context
   * @param edgeDeploymentId The cloud edge deployment ID to check
   * @return Status.OK if the deployment is not in use, otherwise an error status
   */
  public Status validateEdgeDeploymentNotInUse(RequestContext ctx, String edgeDeploymentId) {
    try {
      // Get all bot configurations with edge reference
      List<CloudBotDeploymentConfig> botConfigs =
          ctx.call(
              () ->
                  cloudBotDeploymentConfigService
                      .getCloudBotDeploymentConfigs(
                          GetCloudBotDeploymentConfigsRequest.newBuilder()
                              .setEdgeDeploymentId(edgeDeploymentId)
                              .build())
                      .getCloudBotDeploymentsList());

      if (!botConfigs.isEmpty()) {
        String referencingBotIds =
            botConfigs.stream()
                .map(CloudBotDeploymentConfig::getId)
                .collect(Collectors.joining(", "));

        return Status.FAILED_PRECONDITION.withDescription(
            "Edge deployment with ID "
                + edgeDeploymentId
                + " is in use by bot deployment(s): "
                + referencingBotIds
                + " and cannot be deleted");
      }

      return Status.OK;
    } catch (Exception e) {
      log.error("Error validating edge deployment usage for ID: {}", edgeDeploymentId, e);
      return Status.INTERNAL.withDescription(
          "Failed to validate edge deployment usage: " + e.getMessage());
    }
  }
}
