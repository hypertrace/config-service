package ai.traceable.cloud.bot.deployment.config.service.v1.manager;

import static ai.traceable.config.utils.RegexValidator.validateRegex;

import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaProviderDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaType;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigInput;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.CreateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentMode;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.EdgeDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.IpWhitelistConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.RuleType;
import ai.traceable.cloud.bot.deployment.config.service.v1.SiteConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentStatusRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.UrlRule;
import ai.traceable.platform.utils.ip.IpValidationUtils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.validator.routines.DomainValidator;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CloudBotDeploymentConfigValidator {

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

  public Status validate(
      UpdateCloudBotDeploymentConfigRequest request, CloudBotDeploymentConfig existingConfig) {
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

    if (input.getDeploymentDetails().getDeploymentMode()
        != existingConfig.getDeploymentDetails().getDeploymentMode()) {
      throw new StatusRuntimeException(
          Status.INVALID_ARGUMENT.withDescription("Cannot change deployment mode"));
    }

    // Do not allow users to update site-key config
    if (!input.getSiteConfig().getSiteKey().isEmpty()
        && !input
            .getSiteConfig()
            .getSiteKey()
            .equals(existingConfig.getSiteConfig().getSiteKey())) {
      throw new StatusRuntimeException(
          Status.INVALID_ARGUMENT.withDescription("Cannot change site key id"));
    }

    return Status.OK;
  }

  public Status validate(
      UpdateCloudBotDeploymentStatusRequest request, CloudBotDeploymentConfig existingConfig) {
    Status status = validateCloudBotDeploymentStatus(request.getCloudBotDeploymentStatus());
    if (!status.isOk()) {
      return status;
    }

    // If cluster is being provisioned for the first time, ensure all details are provided
    if (request.getCloudBotDeploymentStatus().getDeploymentStatus()
            == DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY
        && existingConfig.getCloudBotDeploymentStatus().getDeploymentStatus()
            == DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED) {
      return validateDeploymentDetailsForClusterCreation(request, existingConfig);
    }

    return Status.OK;
  }

  Status validateId(String id) {
    if (id.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Cloud bot deployment config ID cannot be empty");
    }
    return Status.OK;
  }

  private Status validateSiteConfig(SiteConfig siteConfig) {
    if (siteConfig.getSiteName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Site name cannot be empty");
    }

    Status status = validate(siteConfig.getIpWhitelistConfig());
    if (!status.isOk()) {
      return status;
    }

    if (siteConfig.getDomainsCount() == 0) {
      return Status.INVALID_ARGUMENT.withDescription("At least one domain must be specified");
    }

    // Validate each domain
    for (String domain : siteConfig.getDomainsList()) {
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
      status = validateCaptchaConfig(siteConfig.getCaptchaConfig());
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

    if (captchaConfig.getEnabled()) {

      for (UrlRule rule : captchaConfig.getUrlRulesList()) {
        if (rule.getRuleType().equals(RuleType.RULE_TYPE_EXCLUDE_URL_REGEX)
            || rule.getRuleType().equals(RuleType.RULE_TYPE_INCLUDE_URL_REGEX)) {

          for (String urlRegex : rule.getValuesList()) {
            Status status = validateRegex(urlRegex);
            if (!status.isOk()) {
              return status;
            }
          }
        }
      }
    }

    return Status.OK;
  }

  private static Status validateDeploymentDetails(DeploymentDetails deploymentDetails) {
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

  private static Status validateEdgeDeploymentConfig(EdgeDeploymentConfig edgeConfig) {
    if (edgeConfig.getCloudEdgeDeploymentId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Associated cloud edge deployment ID cannot be empty");
    }

    return Status.OK;
  }

  private static Status validateCloudBotDeploymentStatus(CloudBotDeploymentStatus status) {
    if (status.getDeploymentStatus() == DeploymentStatus.DEPLOYMENT_STATUS_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription("Deployment status cannot be UNSPECIFIED");
    }
    if (status.getDeploymentStatus() == DeploymentStatus.DEPLOYMENT_STATUS_BLOCKED
        && !status.hasMessage()
        && status.getMessage().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Blocked deployment status should have a reason for being blocked, which will be shown to customers");
    }
    return Status.OK;
  }

  private static Status validateDeploymentDetailsForClusterCreation(
      UpdateCloudBotDeploymentStatusRequest request, CloudBotDeploymentConfig existingConfig) {
    if (request.hasCaptchaProviderDetails()) {
      Status status = validateCaptchaProviderDetails(request.getCaptchaProviderDetails());
      if (!status.isOk()) {
        return status;
      }
    }

    if (existingConfig.getDeploymentDetails().getDeploymentMode()
            == DeploymentMode.DEPLOYMENT_MODE_OUT_OF_BAND
        && !request.hasTraceableCaptchaDomain()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Traceable bot deployment cluster domain cannot be empty for OOB deployment");
    }
    return Status.OK;
  }

  private static Status validateCaptchaProviderDetails(CaptchaProviderDetails details) {
    if (details.hasMtCaptcha() && details.getMtCaptcha().getSiteKey().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("MTCaptcha site key cannot be empty");
    }

    return Status.OK;
  }

  private Status validate(IpWhitelistConfig ipWhitelistConfig) {
    // Validate IP addresses
    for (String ip : ipWhitelistConfig.getIpAddressesList()) {
      if (!IpValidationUtils.isValidIpAddress(ip)) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format("IP address %s not valid", ip));
      }
    }

    // Validate IP ranges (CIDR notation)
    for (String cidr : ipWhitelistConfig.getIpRangesList()) {
      if (!IpValidationUtils.isValidSubnet(cidr)) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format("IP range %s not valid", cidr));
      }
    }

    return Status.OK;
  }
}
