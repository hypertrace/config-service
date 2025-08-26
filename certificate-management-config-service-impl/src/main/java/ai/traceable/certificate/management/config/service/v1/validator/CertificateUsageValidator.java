package ai.traceable.certificate.management.config.service.v1.validator;

import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfigServiceGrpc.CloudEdgeDeploymentConfigServiceBlockingStub;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfigWithActions;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.GetCloudEdgeDeploymentConfigsFilter;
import ai.traceable.cloud.edge.deployment.config.service.v1.GetCloudEdgeDeploymentConfigsRequest;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

/** Validator for checking certificate usage in other services. */
@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CertificateUsageValidator {

  private final CloudEdgeDeploymentConfigServiceBlockingStub cloudEdgeDeploymentConfigService;

  /**
   * Validates if a certificate is not in use by any cloud-edge deployment.
   *
   * @param ctx The request context
   * @param certificateId The certificate ID to check
   * @return Status.OK if the certificate is not in use, otherwise an error status
   */
  public Status validateCertificateNotInUse(RequestContext ctx, String certificateId) {
    try {
      // Get cloud-edge deployments that use this certificate ID
      GetCloudEdgeDeploymentConfigsRequest request =
          GetCloudEdgeDeploymentConfigsRequest.newBuilder()
              .setReadAccess(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
              .setFilter(
                  GetCloudEdgeDeploymentConfigsFilter.newBuilder()
                      .addCertificateIds(certificateId)
                      .build())
              .build();

      List<CloudEdgeDeploymentConfig> deployments =
          cloudEdgeDeploymentConfigService
              .getCloudEdgeDeploymentConfigs(request)
              .getCloudEdgeDeploymentsWithActionsList()
              .stream()
              .map(CloudEdgeDeploymentConfigWithActions::getCloudEdgeDeploymentConfig)
              .collect(Collectors.toList());

      // If any deployments are returned, the certificate is in use
      if (!deployments.isEmpty()) {
        CloudEdgeDeploymentConfig deployment = deployments.get(0);
        return Status.FAILED_PRECONDITION.withDescription(
            String.format(
                "Certificate with ID %s is in use by cloud-edge deployment %s and cannot be deleted",
                certificateId, deployment.getId()));
      }

      return Status.OK;
    } catch (Exception e) {
      log.error("Error validating certificate usage for ID: {}", certificateId, e);
      return Status.INTERNAL.withDescription(
          "Failed to validate certificate usage: " + e.getMessage());
    }
  }
}
