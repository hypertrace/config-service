package ai.traceable.cloud.bot.deployment.config.service.v1.manager;

import ai.traceable.cloud.bot.deployment.config.service.v1.ApiToken;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigInput;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.ClusterStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.CreateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeleteCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentMode;
import ai.traceable.cloud.bot.deployment.config.service.v1.SiteConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentStatusRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.store.CloudBotDeploymentConfigStore;
import ai.traceable.config.utils.UuidGenerator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CloudBotDeploymentConfigManagerImpl implements CloudBotDeploymentConfigManager {

  private final CloudBotDeploymentConfigStore store;
  private final CloudBotDeploymentConfigValidator validator;
  private final UuidGenerator uuidGenerator;
  private final KeyPairGenerator keyPairGenerator;

  @Override
  public CloudBotDeploymentConfig createCloudBotDeploymentConfig(
      RequestContext ctx, CreateCloudBotDeploymentConfigRequest request) {
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw new StatusRuntimeException(validationStatus);
    }

    // Generate a unique ID
    String id = uuidGenerator.generateRandomId();

    CloudBotDeploymentConfigInput input = request.getCloudBotDeploymentConfigInput();
    SiteConfig.Builder siteConfigBuilder = input.getSiteConfig().toBuilder();

    // Set a random site-key when creating the request
    siteConfigBuilder.setSiteKey(uuidGenerator.generateRandomId());

    // Generate a public-private key pair and set it for signing jwt
    siteConfigBuilder.setJwtSigningDetails(keyPairGenerator.generateJwtSigningKeyPair());

    // Set API token for out-of-band deployments
    if (input.getDeploymentDetails().getDeploymentMode()
        == DeploymentMode.DEPLOYMENT_MODE_OUT_OF_BAND) {
      String agentToken = uuidGenerator.generateRandomId();
      DeploymentDetails updatedDeploymentDetails =
          input.getDeploymentDetails().toBuilder()
              .setOobDeploymentConfig(
                  input.getDeploymentDetails().getOobDeploymentConfig().toBuilder()
                      .setApiToken(ApiToken.newBuilder().setKeyValue(agentToken)))
              .build();

      // Update the input with the new deployment details
      input = input.toBuilder().setDeploymentDetails(updatedDeploymentDetails).build();
    }

    // Create a new config with provisioning status
    CloudBotDeploymentConfig config =
        CloudBotDeploymentConfig.newBuilder()
            .setId(id)
            .setSiteConfig(siteConfigBuilder)
            .setDeploymentDetails(input.getDeploymentDetails())
            .setCloudBotDeploymentStatus(
                CloudBotDeploymentStatus.newBuilder()
                    .setClusterStatus(ClusterStatus.CLUSTER_STATUS_PROVISIONING))
            .build();

    return store.createCloudBotDeploymentConfig(ctx, config);
  }

  @Override
  public CloudBotDeploymentConfig updateCloudBotDeploymentConfig(
      RequestContext ctx, String id, UpdateCloudBotDeploymentConfigRequest request) {
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw new StatusRuntimeException(validationStatus);
    }

    // Check if config with the given ID exists
    CloudBotDeploymentConfig existingConfig = store.getCloudBotDeploymentConfig(ctx, id);
    if (existingConfig == null) {
      throw new StatusRuntimeException(
          Status.NOT_FOUND.withDescription("Cloud bot deployment config not found"));
    }

    try {
      CloudBotDeploymentConfigInput requestedInput = request.getCloudBotDeploymentConfigInput();

      // Start with existing config and update only what's needed
      CloudBotDeploymentConfig.Builder updatedConfigBuilder = existingConfig.toBuilder();

      // Update deployment details
      updatedConfigBuilder.setDeploymentDetails(
          updateDeploymentDetails(
              existingConfig.getDeploymentDetails(), requestedInput.getDeploymentDetails()));

      // Update site config
      updatedConfigBuilder.setSiteConfig(
          getUpdatedSiteConfig(existingConfig.getSiteConfig(), requestedInput.getSiteConfig()));

      // Save and return updated config
      return store.updateCloudBotDeploymentConfig(ctx, updatedConfigBuilder.build());
    } catch (StatusRuntimeException e) {
      throw e; // Re-throw status exceptions as-is
    } catch (Exception e) {
      throw new StatusRuntimeException(
          Status.INTERNAL.withDescription(
              "Failed to update cloud bot deployment config: " + e.getMessage()));
    }
  }

  private SiteConfig getUpdatedSiteConfig(
      SiteConfig existingSiteConfig, SiteConfig requestedSiteConfig) {
    // Start with existing changes
    SiteConfig.Builder updatedSiteConfig = existingSiteConfig.toBuilder();

    if (!requestedSiteConfig.getSiteKey().isEmpty()
        && !existingSiteConfig.getSiteKey().equals(requestedSiteConfig.getSiteKey())) {
      throw new StatusRuntimeException(
          Status.INVALID_ARGUMENT.withDescription("Cannot change site key id"));
    }

    // Protecting change is list of domains as it is passed to MTCaptcha as well
    if (!existingSiteConfig.getDomainsList().equals(requestedSiteConfig.getDomainsList())) {
      throw new StatusRuntimeException(
          Status.INVALID_ARGUMENT.withDescription("Cannot change list of domains to be protected"));
    }

    updatedSiteConfig.setSiteName(requestedSiteConfig.getSiteName());
    if (requestedSiteConfig.getCaptchaConfig() != updatedSiteConfig.getCaptchaConfig()) {
      updatedSiteConfig.setCaptchaConfig(requestedSiteConfig.getCaptchaConfig());
    }
    if (requestedSiteConfig.getIpWhitelistConfig() != updatedSiteConfig.getIpWhitelistConfig()) {
      updatedSiteConfig.setIpWhitelistConfig(requestedSiteConfig.getIpWhitelistConfig());
    }

    return updatedSiteConfig.build();
  }

  private DeploymentDetails updateDeploymentDetails(
      DeploymentDetails existingDetails, DeploymentDetails requestedDetails) {
    if (requestedDetails.getDeploymentMode() != existingDetails.getDeploymentMode()) {
      throw new StatusRuntimeException(
          Status.INVALID_ARGUMENT.withDescription("Cannot change deployment mode"));
    }

    // Start with existing changes
    DeploymentDetails.Builder updatedDetails = existingDetails.toBuilder();
    updatedDetails.setEnvironment(requestedDetails.getEnvironment());

    // Not updating OOB as tokens cannot be updated by users
    if (existingDetails.getDeploymentMode() == DeploymentMode.DEPLOYMENT_MODE_EDGE) {
      updatedDetails.setEdgeDeploymentConfig(requestedDetails.getEdgeDeploymentConfig());
    }

    return updatedDetails.build();
  }

  @Override
  public CloudBotDeploymentConfig updateCloudBotDeploymentStatus(
      RequestContext ctx, String id, UpdateCloudBotDeploymentStatusRequest request) {
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw new StatusRuntimeException(validationStatus);
    }

    // Check if config with the given ID exists
    CloudBotDeploymentConfig existingConfig = store.getCloudBotDeploymentConfig(ctx, id);
    if (existingConfig == null) {
      throw new StatusRuntimeException(
          Status.NOT_FOUND.withDescription("Cloud bot deployment config not found"));
    }

    try {
      // Update status
      CloudBotDeploymentConfig.Builder updatedConfigBuilder = existingConfig.toBuilder();

      // Update deployment status
      if (request.hasCloudBotDeploymentStatus()) {
        updatedConfigBuilder.setCloudBotDeploymentStatus(request.getCloudBotDeploymentStatus());
      }

      // Update captcha provider details if provided
      if (request.hasCaptchaProviderDetails()) {
        updatedConfigBuilder.setSiteConfig(
            existingConfig.getSiteConfig().toBuilder()
                .setCaptchaProviderDetails(request.getCaptchaProviderDetails())
                .build());
      }

      CloudBotDeploymentConfig updatedConfig = updatedConfigBuilder.build();
      return store.updateCloudBotDeploymentConfig(ctx, updatedConfig);
    } catch (Exception e) {
      throw new StatusRuntimeException(
          Status.INTERNAL.withDescription(
              "Failed to update cloud bot deployment status: " + e.getMessage()));
    }
  }

  @Override
  public void deleteCloudBotDeploymentConfig(RequestContext ctx, String id) {
    DeleteCloudBotDeploymentConfigRequest request =
        DeleteCloudBotDeploymentConfigRequest.newBuilder().setId(id).build();
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }

    // Check if config exists
    CloudBotDeploymentConfig config = store.getCloudBotDeploymentConfig(ctx, id);
    if (config == null) {
      throw Status.NOT_FOUND
          .withDescription("Cloud bot deployment config not found with id: " + id)
          .asRuntimeException();
    }

    try {
      store.deleteCloudBotDeploymentConfig(ctx, id);
    } catch (Exception e) {
      throw new StatusRuntimeException(
          Status.INTERNAL.withDescription(
              "Failed to delete cloud bot deployment config: " + e.getMessage()));
    }
  }

  @Override
  public List<CloudBotDeploymentConfig> getCloudBotDeploymentConfigs(
      RequestContext ctx, List<String> ids) {
    if (ids.isEmpty()) {
      return store.getAllCloudBotDeploymentConfigs(ctx);
    }

    return ids.stream()
        .map(id -> store.getCloudBotDeploymentConfig(ctx, id))
        .collect(Collectors.toList());
  }
}
