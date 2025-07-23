package ai.traceable.cloud.bot.deployment.config.service.v1.manager;

import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaProviderDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaType;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigInput;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.ClusterStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.CreateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeleteCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentMode;
import ai.traceable.cloud.bot.deployment.config.service.v1.EdgeDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.SiteConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentStatusRequest;
import io.grpc.Status;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.validator.routines.DomainValidator;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CloudBotDeploymentConfigValidator {

  private static final int MAX_DOMAIN_LENGTH = 253; // Maximum length of a domain name
  private static final DomainValidator DOMAIN_VALIDATOR = DomainValidator.getInstance(false);

  public Status validate(CreateCloudBotDeploymentConfigRequest request) {
    if (!request.hasCloudBotDeploymentConfigInput()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Cloud bot deployment config input is required");
    }

    CloudBotDeploymentConfigInput input = request.getCloudBotDeploymentConfigInput();

    Status status = validateSiteConfig(input.getSiteConfig());
    if (!status.isOk()) {
      return status;
    }

    status = validateDeploymentDetails(input.getDeploymentDetails());
    if (!status.isOk()) {
      return status;
    }

    return Status.OK;
  }

  public Status validate(UpdateCloudBotDeploymentConfigRequest request) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Cloud bot deployment config ID cannot be empty");
    }

    if (!request.hasCloudBotDeploymentConfigInput()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Cloud bot deployment config input is required");
    }

    CloudBotDeploymentConfigInput input = request.getCloudBotDeploymentConfigInput();

    Status status = validateSiteConfig(input.getSiteConfig());
    if (!status.isOk()) {
      return status;
    }

    status = validateDeploymentDetails(input.getDeploymentDetails());
    if (!status.isOk()) {
      return status;
    }

    return Status.OK;
  }

  public Status validate(UpdateCloudBotDeploymentStatusRequest request) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Cloud bot deployment config ID cannot be empty");
    }

    if (request.hasCloudBotDeploymentStatus()) {
      Status status = validateCloudBotDeploymentStatus(request.getCloudBotDeploymentStatus());
      if (!status.isOk()) {
        return status;
      }
    }

    if (request.hasCaptchaProviderDetails()) {
      Status status = validateCaptchaProviderDetails(request.getCaptchaProviderDetails());
      if (!status.isOk()) {
        return status;
      }
    }

    return Status.OK;
  }

  public Status validate(DeleteCloudBotDeploymentConfigRequest request) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Cloud bot deployment config ID cannot be empty");
    }
    return Status.OK;
  }

  private Status validateSiteConfig(SiteConfig siteConfig) {
    if (siteConfig.getSiteName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Site name cannot be empty");
    }

    if (siteConfig.getDomainsCount() == 0) {
      return Status.INVALID_ARGUMENT.withDescription("At least one domain must be specified");
    }

    // Validate each domain
    for (String domain : siteConfig.getDomainsList()) {
      // Check domain length
      if (domain.length() > MAX_DOMAIN_LENGTH) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Domain exceeds maximum length of " + MAX_DOMAIN_LENGTH + " characters: " + domain);
      }

      // Handle wildcard domains
      if (domain.startsWith("*.")) {
        String domainWithoutWildcard = domain.substring(2);
        if (!DOMAIN_VALIDATOR.isValid(domainWithoutWildcard)) {
          return Status.INVALID_ARGUMENT.withDescription(
              "Invalid wildcard domain format: " + domain);
        }
      } else if (!DOMAIN_VALIDATOR.isValid(domain)) {
        return Status.INVALID_ARGUMENT.withDescription("Invalid domain format: " + domain);
      }
    }

    if (siteConfig.hasCaptchaConfig()) {
      Status status = validateCaptchaConfig(siteConfig.getCaptchaConfig());
      if (!status.isOk()) {
        return status;
      }
    }

    return Status.OK;
  }

  private Status validateCaptchaConfig(CaptchaConfig captchaConfig) {
    if (captchaConfig.getCaptchaType() == CaptchaType.CAPTCHA_TYPE_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription("Captcha type must be specified");
    }

    return Status.OK;
  }

  private Status validateDeploymentDetails(DeploymentDetails deploymentDetails) {
    if (deploymentDetails.getEnvironment().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Environment cannot be empty");
    }

    if (deploymentDetails.getDeploymentMode() == DeploymentMode.DEPLOYMENT_MODE_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription("Deployment mode must be specified");
    }

    // Validate deployment-specific config based on mode
    if (deploymentDetails.getDeploymentMode() == DeploymentMode.DEPLOYMENT_MODE_EDGE) {
      if (!deploymentDetails.hasEdgeDeploymentConfig()) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Edge deployment config is required for EDGE deployment mode");
      }

      Status status = validateEdgeDeploymentConfig(deploymentDetails.getEdgeDeploymentConfig());
      if (!status.isOk()) {
        return status;
      }
    } else if (deploymentDetails.getDeploymentMode()
        == DeploymentMode.DEPLOYMENT_MODE_OUT_OF_BAND) {
      if (!deploymentDetails.hasOobDeploymentConfig()) {
        return Status.INVALID_ARGUMENT.withDescription(
            "OOB deployment config is required for OUT_OF_BAND deployment mode");
      }
    }

    return Status.OK;
  }

  private Status validateEdgeDeploymentConfig(EdgeDeploymentConfig edgeConfig) {
    if (edgeConfig.getCloudEdgeDeploymentId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Associated cloud edge deployment ID cannot be empty");
    }

    return Status.OK;
  }

  private Status validateCloudBotDeploymentStatus(CloudBotDeploymentStatus status) {
    if (status.getClusterStatus() == ClusterStatus.CLUSTER_STATUS_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription("Cluster status cannot be UNSPECIFIED");
    }

    return Status.OK;
  }

  private Status validateCaptchaProviderDetails(CaptchaProviderDetails details) {
    if (details.hasMtCaptcha() && details.getMtCaptcha().getSiteKey().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("MTCaptcha site key cannot be empty");
    }

    return Status.OK;
  }
}
