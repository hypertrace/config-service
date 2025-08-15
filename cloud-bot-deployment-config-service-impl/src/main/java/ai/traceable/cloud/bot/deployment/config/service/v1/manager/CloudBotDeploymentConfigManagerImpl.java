package ai.traceable.cloud.bot.deployment.config.service.v1.manager;

import ai.traceable.cloud.bot.deployment.config.service.v1.ApiToken;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigInput;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.CreateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentMode;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.EnableCloudBotDeploymentRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.IpWhitelistConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.OobDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.OobDeploymentConfig.Builder;
import ai.traceable.cloud.bot.deployment.config.service.v1.SiteConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentStatusRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.encryption.KeyPairGenerator;
import ai.traceable.cloud.bot.deployment.config.service.v1.state.transitions.Action;
import ai.traceable.cloud.bot.deployment.config.service.v1.state.transitions.StateTransitionsRegistry;
import ai.traceable.cloud.bot.deployment.config.service.v1.store.CloudBotDeploymentConfigStore;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.platform.utils.ip.IpAddressParsingUtils;
import ai.traceable.platform.utils.ip.IpAddressParsingUtils.IpParsingResults;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Inject;
import java.nio.file.AccessDeniedException;
import java.time.Clock;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CloudBotDeploymentConfigManagerImpl implements CloudBotDeploymentConfigManager {

  private final CloudBotDeploymentConfigStore store;
  private final CloudBotDeploymentConfigValidator validator;
  private final StateTransitionsRegistry stateTransitionsRegistry;
  private final UuidGenerator uuidGenerator;
  private final KeyPairGenerator keyPairGenerator;
  private final Clock clock;

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

    // Standardize ips
    if (siteConfigBuilder.hasIpWhitelistConfig()) {
      siteConfigBuilder.setIpWhitelistConfig(
          standardizeIps(siteConfigBuilder.getIpWhitelistConfig()));
    }

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

    // Get the next state for the creation action
    CloudBotDeploymentStatus nextState =
        checkAndSetDeploymentState(
            id, DeploymentStatus.DEPLOYMENT_STATUS_UNSPECIFIED, Action.ACTION_CREATE);

    // Create a new config with the appropriate status
    CloudBotDeploymentConfig config =
        CloudBotDeploymentConfig.newBuilder()
            .setEnabled(input.getEnabled())
            .setId(id)
            .setSiteConfig(siteConfigBuilder)
            .setDeploymentDetails(input.getDeploymentDetails())
            .setCloudBotDeploymentStatus(nextState)
            .setLastUpdatedTimestamp(Timestamp.newBuilder().setSeconds(this.clock.millis() / 1000))
            .build();

    return store.createCloudBotDeploymentConfig(ctx, config);
  }

  @Override
  public CloudBotDeploymentConfig updateCloudBotDeploymentConfig(
      RequestContext ctx, String id, UpdateCloudBotDeploymentConfigRequest request) {
    CloudBotDeploymentConfig existingConfig = this.getCloudBotDeploymentConfig(ctx, id);

    Status validationStatus = validator.validate(request, existingConfig);
    if (!validationStatus.isOk()) {
      throw new StatusRuntimeException(validationStatus);
    }

    try {
      CloudBotDeploymentConfigInput requestedInput = request.getCloudBotDeploymentConfigInput();

      CloudBotDeploymentStatus nextState =
          isBreakingChange(existingConfig, requestedInput)
              ? checkAndSetDeploymentState(
                  id,
                  existingConfig.getCloudBotDeploymentStatus().getDeploymentStatus(),
                  Action.ACTION_EDIT_BREAKING)
              : existingConfig.getCloudBotDeploymentStatus();

      // Start with existing config and update only what's needed
      CloudBotDeploymentConfig.Builder updatedConfigBuilder = existingConfig.toBuilder();

      // Update deployment details
      updatedConfigBuilder.setDeploymentDetails(
          updateDeploymentDetails(
              existingConfig.getDeploymentDetails(), requestedInput.getDeploymentDetails()));

      // Update site config
      updatedConfigBuilder.setSiteConfig(
          getUpdatedSiteConfig(existingConfig.getSiteConfig(), requestedInput.getSiteConfig()));

      updatedConfigBuilder
          .setEnabled(requestedInput.getEnabled())
          .setCloudBotDeploymentStatus(nextState)
          .setLastUpdatedTimestamp(Timestamp.newBuilder().setSeconds(this.clock.millis() / 1000));

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

  @Override
  public CloudBotDeploymentConfig updateCloudBotDeploymentStatus(
      RequestContext ctx, String id, UpdateCloudBotDeploymentStatusRequest request) {
    CloudBotDeploymentConfig existingConfig = this.getCloudBotDeploymentConfig(ctx, id);

    Status validationStatus = validator.validate(request, existingConfig);
    if (!validationStatus.isOk()) {
      throw new StatusRuntimeException(validationStatus);
    }

    try {
      CloudBotDeploymentConfig.Builder updatedConfigBuilder = existingConfig.toBuilder();

      // Set the next state from state transitions
      if (request.hasCloudBotDeploymentStatus()) {
        // Check and set deployment state using state transitions registry
        CloudBotDeploymentStatus nextState =
            checkAndSetDeploymentState(
                id,
                existingConfig.getCloudBotDeploymentStatus().getDeploymentStatus(),
                request.getCloudBotDeploymentStatus().getDeploymentStatus()
                        == DeploymentStatus.DEPLOYMENT_STATUS_BLOCKED
                    ? Action.ACTION_BLOCK
                    : Action.ACTION_UPDATE_STATUS);

        if (nextState.getDeploymentStatus()
            != request.getCloudBotDeploymentStatus().getDeploymentStatus()) {
          throw new StatusRuntimeException(
              Status.PERMISSION_DENIED.withDescription(
                  "Cannot transition from "
                      + existingConfig.getCloudBotDeploymentStatus().getDeploymentStatus()
                      + " to "
                      + request.getCloudBotDeploymentStatus().getDeploymentStatus()
                      + ". Invalid state transition."));
        }

        // If the requested status is DEPLOYMENT_DELETED, delete the config instead of updating
        if (request.getCloudBotDeploymentStatus().getDeploymentStatus()
            == DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYMENT_DELETED) {
          store.deleteCloudBotDeploymentConfig(ctx, id);
          // Return the config with updated status before deletion for confirmation
          return existingConfig.toBuilder()
              .setCloudBotDeploymentStatus(request.getCloudBotDeploymentStatus())
              .build();
        }

        updatedConfigBuilder.setCloudBotDeploymentStatus(request.getCloudBotDeploymentStatus());
      }

      // Update captcha provider details if provided
      if (request.hasCaptchaProviderDetails()) {
        updatedConfigBuilder.setSiteConfig(
            existingConfig.getSiteConfig().toBuilder()
                .setCaptchaProviderDetails(request.getCaptchaProviderDetails())
                .build());
      }

      if (existingConfig.getDeploymentDetails().getDeploymentMode()
          == DeploymentMode.DEPLOYMENT_MODE_OUT_OF_BAND) {
        OobDeploymentConfig oobDeploymentConfig =
            existingConfig.getDeploymentDetails().getOobDeploymentConfig().toBuilder()
                .setTraceableCaptchaDomain(request.getTraceableCaptchaDomain())
                .build();
        updatedConfigBuilder.setDeploymentDetails(
            existingConfig.getDeploymentDetails().toBuilder()
                .setOobDeploymentConfig(oobDeploymentConfig));
      }

      if (request.hasLabelsUpdate()) {
        updatedConfigBuilder.setSiteConfig(
            updatedConfigBuilder.getSiteConfig().toBuilder()
                .putAllLabels(request.getLabelsUpdate().getMapMap()));
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
    // Check if config exists
    CloudBotDeploymentConfig existingConfig = this.getCloudBotDeploymentConfig(ctx, id);

    CloudBotDeploymentStatus nextState =
        checkAndSetDeploymentState(
            id,
            existingConfig.getCloudBotDeploymentStatus().getDeploymentStatus(),
            Action.ACTION_DELETE);

    try {
      // If it's an explicit delete only then delete the config
      if (nextState.getDeploymentStatus()
          == DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYMENT_DELETED) {
        store.deleteCloudBotDeploymentConfig(ctx, id);
      } else {
        // Just update the state of the config
        CloudBotDeploymentConfig updatedConfig =
            existingConfig.toBuilder().setCloudBotDeploymentStatus(nextState).build();
        store.updateCloudBotDeploymentConfig(ctx, updatedConfig);
      }
    } catch (Exception e) {
      throw new StatusRuntimeException(
          Status.INTERNAL.withDescription(
              "Failed to delete cloud bot deployment config: " + e.getMessage()));
    }
  }

  @Override
  public CloudBotDeploymentConfig rotateApiToken(RequestContext ctx, String id) {
    try {
      CloudBotDeploymentConfig existingConfig = this.getCloudBotDeploymentConfig(ctx, id);

      // Check if this is an out-of-band deployment
      if (existingConfig.getDeploymentDetails().getDeploymentMode()
          != DeploymentMode.DEPLOYMENT_MODE_OUT_OF_BAND) {
        throw new StatusRuntimeException(
            Status.INVALID_ARGUMENT.withDescription(
                "API token rotation is only supported for out-of-band deployments"));
      }

      Builder updatedOobConfigBuilder =
          existingConfig.getDeploymentDetails().getOobDeploymentConfig().toBuilder();

      // Set current token as previous token for 7 days
      if (updatedOobConfigBuilder.hasApiToken()) {
        long expiryTimeMillis = System.currentTimeMillis() + (7 * 24 * 60 * 60 * 1000); // 7 days
        updatedOobConfigBuilder.setPreviousApiToken(
            ApiToken.newBuilder()
                .setKeyValue(updatedOobConfigBuilder.getApiToken().getKeyValue())
                .setExpiryTimestampMillis(expiryTimeMillis)
                .build());
      }

      // Set new api token
      updatedOobConfigBuilder.setApiToken(
          ApiToken.newBuilder().setKeyValue(uuidGenerator.generateRandomId()).build());

      // Create the updated config
      CloudBotDeploymentConfig updatedConfig =
          existingConfig.toBuilder()
              .setDeploymentDetails(
                  existingConfig.getDeploymentDetails().toBuilder()
                      .setOobDeploymentConfig(updatedOobConfigBuilder))
              .build();

      return store.updateCloudBotDeploymentConfig(ctx, updatedConfig);
    } catch (StatusRuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new StatusRuntimeException(
          Status.INTERNAL.withDescription("Failed to rotate API token: " + e.getMessage()));
    }
  }

  @Override
  public List<CloudBotDeploymentConfig> getCloudBotDeploymentConfigs(
      RequestContext ctx, List<String> ids) {
    if (ids.isEmpty()) {
      return store.getAllCloudBotDeploymentConfigs(ctx);
    }

    return ids.stream()
        .map(id -> this.getCloudBotDeploymentConfig(ctx, id))
        .collect(Collectors.toList());
  }

  @Override
  public void enableCloudBotDeployment(
      RequestContext ctx, EnableCloudBotDeploymentRequest request) {
    // Check if config exists
    CloudBotDeploymentConfig existingConfig =
        this.getCloudBotDeploymentConfig(ctx, request.getId());

    try {
      CloudBotDeploymentConfig updatedConfig =
          existingConfig.toBuilder().setEnabled(request.getEnabled()).build();
      store.updateCloudBotDeploymentConfig(ctx, updatedConfig);
    } catch (Exception e) {
      throw new StatusRuntimeException(
          Status.INTERNAL.withDescription(
              "Failed to execute enableCloudBotDeployment request: " + e.getMessage()));
    }
  }

  private CloudBotDeploymentConfig getCloudBotDeploymentConfig(RequestContext ctx, String id) {
    Status validationStatus = validator.validateId(id);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }

    // Get the existing config
    CloudBotDeploymentConfig existingConfig = store.getCloudBotDeploymentConfig(ctx, id);
    if (existingConfig == null) {
      throw new StatusRuntimeException(
          Status.NOT_FOUND.withDescription("Cloud bot deployment config not found: " + id));
    }
    return existingConfig;
  }

  private CloudBotDeploymentStatus checkAndSetDeploymentState(
      String id, DeploymentStatus currentState, Action action) {
    try {
      return CloudBotDeploymentStatus.newBuilder()
          .setDeploymentStatus(stateTransitionsRegistry.checkAndGetNextState(currentState, action))
          .build();
    } catch (AccessDeniedException exception) {
      throw new StatusRuntimeException(
          Status.PERMISSION_DENIED.withDescription(
              String.format(
                  "%s is not allowed for %s for cloud bot deployment: %s",
                  action, currentState, id)));
    }
  }

  private static boolean isBreakingChange(
      CloudBotDeploymentConfig currentConfig, CloudBotDeploymentConfigInput requestInput) {
    // Domain name addition is a breaking change as it would require us to add domain in MT captcha

    // If requestedDomains contains any domain not in currentDomains, it's a breaking change
    return requestInput.getSiteConfig().getDomainsList().stream()
        .anyMatch(domain -> !currentConfig.getSiteConfig().getDomainsList().contains(domain));
  }

  private static IpWhitelistConfig standardizeIps(IpWhitelistConfig ipWhitelistConfig) {
    IpParsingResults ipParsingResults =
        IpAddressParsingUtils.parseRawIpRange(
            Stream.concat(
                    ipWhitelistConfig.getIpAddressesList().stream(),
                    ipWhitelistConfig.getIpRangesList().stream())
                .collect(Collectors.toUnmodifiableList()));
    return IpWhitelistConfig.newBuilder()
        .addAllIpAddresses(ipParsingResults.getIpAddresses())
        .addAllIpRanges(ipParsingResults.getIpRanges())
        .build();
  }

  private static SiteConfig getUpdatedSiteConfig(
      SiteConfig existingSiteConfig, SiteConfig requestedSiteConfig) {
    // Start with existing changes
    SiteConfig.Builder updatedSiteConfig = existingSiteConfig.toBuilder();

    updatedSiteConfig.setSiteName(requestedSiteConfig.getSiteName());

    // Compare captcha config by checking if it exists in the request
    if (requestedSiteConfig.hasCaptchaConfig()) {
      updatedSiteConfig.setCaptchaConfig(requestedSiteConfig.getCaptchaConfig());
    }

    // Compare IP whitelist config by checking if it exists in the request
    if (requestedSiteConfig.hasIpWhitelistConfig()) {
      updatedSiteConfig.setIpWhitelistConfig(
          standardizeIps(requestedSiteConfig.getIpWhitelistConfig()));
    }

    if (requestedSiteConfig.getDomainsCount() != 0) {
      // Clear existing domains and add all requested domains
      updatedSiteConfig.clearDomains();
      updatedSiteConfig.addAllDomains(requestedSiteConfig.getDomainsList());
    }

    return updatedSiteConfig.build();
  }

  private static DeploymentDetails updateDeploymentDetails(
      DeploymentDetails existingDetails, DeploymentDetails requestedDetails) {
    // Start with existing changes
    DeploymentDetails.Builder updatedDetails = existingDetails.toBuilder();
    updatedDetails.setEnvironment(requestedDetails.getEnvironment());

    // Not updating OOB as tokens cannot be updated by users
    if (existingDetails.getDeploymentMode() == DeploymentMode.DEPLOYMENT_MODE_EDGE) {
      updatedDetails.setEdgeDeploymentConfig(requestedDetails.getEdgeDeploymentConfig());
    }

    return updatedDetails.build();
  }
}
