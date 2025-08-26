package ai.traceable.cloud.edge.deployment.config.service.v1.validator;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.cloud.edge.deployment.config.service.v1.Action;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfigActionRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentInputConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigPermission;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigValueDescriptor;
import ai.traceable.cloud.edge.deployment.config.service.v1.CreateCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeleteCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeploymentStatus;
import ai.traceable.cloud.edge.deployment.config.service.v1.DomainConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.HealthCheckDetails;
import ai.traceable.cloud.edge.deployment.config.service.v1.HealthCheckSettings;
import ai.traceable.cloud.edge.deployment.config.service.v1.OriginConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.Protocol;
import ai.traceable.cloud.edge.deployment.config.service.v1.ServiceConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.SharedConfigMetadata;
import ai.traceable.cloud.edge.deployment.config.service.v1.SuccessCodeRange;
import ai.traceable.cloud.edge.deployment.config.service.v1.UpdateCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.shared.config.SharedConfigMetadataRegistry;
import ai.traceable.cloud.edge.deployment.config.service.v1.state.transitions.StateTransitionsRegistry;
import com.google.protobuf.Duration;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.validator.routines.DomainValidator;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CloudEdgeDeploymentValidator {
  private static final DomainValidator DOMAIN_VALIDATOR = DomainValidator.getInstance(false);

  private final SharedConfigMetadataRegistry sharedConfigMetadataRegistry;
  private final StateTransitionsRegistry stateTransitionsRegistry;
  private static final String EMPTY_ID_ERROR = "Cloud edge deployment config ID cannot be empty";

  public Status validate(CreateCloudEdgeDeploymentConfigRequest request) {
    if (!request.hasCloudEdgeDeploymentInputConfig()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Cloud edge deployment input config is required");
    }

    if (!request.hasConfigPermission()) {
      return Status.INVALID_ARGUMENT.withDescription("Config permission is required");
    }
    Status status =
        validateInputConfig(
            request.getCloudEdgeDeploymentInputConfig(), request.getConfigPermission());
    if (!status.equals(Status.OK)) {
      return status;
    }

    return Status.OK;
  }

  public Status validate(UpdateCloudEdgeDeploymentConfigRequest request) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(EMPTY_ID_ERROR);
    }

    if (!request.hasConfigPermission()) {
      return Status.INVALID_ARGUMENT.withDescription("Config permission is required");
    }

    if (request.getConfigPermission().getWrite().equals(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
        && request.hasCloudEdgeDeployedOutputConfig()) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "User with permission %s can't update out config", request.getConfigPermission()));
    }
    if (request.hasCloudEdgeDeploymentInputConfig()) {
      Status status =
          validateInputConfig(
              request.getCloudEdgeDeploymentInputConfig(), request.getConfigPermission());
      if (!status.equals(Status.OK)) {
        return status;
      }
    }

    return Status.OK;
  }

  public Status validate(DeleteCloudEdgeDeploymentConfigRequest request) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(EMPTY_ID_ERROR);
    }
    return Status.OK;
  }

  public Status validate(CloudEdgeDeploymentConfigActionRequest request) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(EMPTY_ID_ERROR);
    }

    if (request.getAction() == Action.ACTION_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription("Action cannot be unspecified");
    }

    if (!List.of(
            Action.ACTION_DEPLOY,
            Action.ACTION_HOLD,
            Action.ACTION_REQUEST_REMOVAL,
            Action.ACTION_CANCEL_CHANGE_REQUEST,
            Action.ACTION_CANCEL_REMOVAL_REQUEST)
        .contains(request.getAction())) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid action: " + request.getAction());
    }

    return Status.OK;
  }

  private Status validateInputConfig(
      CloudEdgeDeploymentInputConfig cloudEdgeDeploymentInputConfig,
      ConfigPermission configPermission) {

    Set<String> serviceNames = new HashSet<>();
    // Set to track domain names across all service configs
    Set<String> domainNames = new HashSet<>();

    for (ServiceConfig serviceConfig : cloudEdgeDeploymentInputConfig.getServiceConfigsList()) {
      String serviceName = serviceConfig.getServiceName();
      if (!serviceNames.add(serviceName)) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "Duplicate service name found: %s. Service names must be unique.", serviceName));
      }

      // validate domain uniqueness across all services
      for (DomainConfig domainConfig : serviceConfig.getDomainConfigsList()) {
        String domainName = domainConfig.getDomainName();
        // Validate domain
        validateDomainName(domainName);
        if (!domainNames.add(domainName)) {
          return Status.INVALID_ARGUMENT.withDescription(
              String.format(
                  "Duplicate domain name found: %s. Domain names must be unique across all service configurations.",
                  domainName));
        }

        // Also validate that if protocol is HTTPS, certificate ID cannot be null/empty
        if (domainConfig.getProtocol() == Protocol.PROTOCOL_HTTPS
            && domainConfig.getCertificateId().isEmpty()) {
          return Status.INVALID_ARGUMENT.withDescription(
              String.format(
                  "HTTPS protocol requires a certificate ID for domain %s in service %s",
                  domainName, serviceName));
        }
      }
      try {
        serviceConfig.getOriginConfigsList().forEach(this::validateOriginConfig);
        if (serviceConfig.hasHealthCheckDetails()) {
          validateHealthCheckDetails(serviceConfig.getHealthCheckDetails());
        }
      } catch (StatusRuntimeException ex) {
        return ex.getStatus();
      }
    }

    SharedConfigMetadata sharedConfigMetadata =
        sharedConfigMetadataRegistry.getSharedConfigMetadataWithWritePermission(
            configPermission.getWrite());
    Map<String, ConfigValueDescriptor> clusterConfigDetailMap =
        sharedConfigMetadata.getClusterConfigDetailsMap();
    Map<String, ConfigValueDescriptor> serviceConfigDetailMap =
        sharedConfigMetadata.getServiceConfigDetailsMap();
    for (String key :
        cloudEdgeDeploymentInputConfig
            .getClusterConfig()
            .getAdvancedConfig()
            .getGenericConfig()
            .getFieldsMap()
            .keySet()) {
      if (!clusterConfigDetailMap.containsKey(key)) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "User with permission %s can't update cluster advanced config key %s",
                configPermission, key));
      }
    }
    for (ServiceConfig serviceConfig : cloudEdgeDeploymentInputConfig.getServiceConfigsList()) {
      for (String key :
          serviceConfig.getAdvancedConfig().getGenericConfig().getFieldsMap().keySet()) {
        if (!serviceConfigDetailMap.containsKey(key)) {
          return Status.INVALID_ARGUMENT.withDescription(
              String.format(
                  "User with permission %s can't update service advanced config key %s and service ",
                  configPermission, key));
        }
      }
    }
    return Status.OK;
  }

  private void validateOriginConfig(OriginConfig originConfig) {
    if (originConfig.hasHealthCheckDetails()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Health check cannot be configured at origin config: %s", originConfig))
          .asRuntimeException();
    }
    validateNonDefaultPresenceOrThrow(originConfig, OriginConfig.PROTOCOL_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(originConfig, OriginConfig.PORT_FIELD_NUMBER);
    if (originConfig.hasHostName()) {
      validateNonDefaultPresenceOrThrow(originConfig, OriginConfig.HOST_NAME_FIELD_NUMBER);
    } else if (originConfig.hasIp()) {
      validateNonDefaultPresenceOrThrow(originConfig, OriginConfig.IP_FIELD_NUMBER);
    }
  }

  private void validateHealthCheckDetails(HealthCheckDetails healthCheckDetails) {
    validateNonDefaultPresenceOrThrow(
        healthCheckDetails, HealthCheckDetails.HEALTH_CHECK_PATH_FIELD_NUMBER);
    validateHealthCheckSettings(healthCheckDetails.getHealthCheckSettings());
  }

  private void validateHealthCheckSettings(HealthCheckSettings healthCheckSettings) {
    validateNonDefaultPresenceOrThrow(
        healthCheckSettings, HealthCheckSettings.HEALTHY_THRESHOLD_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        healthCheckSettings, HealthCheckSettings.UNHEALTHY_THRESHOLD_FIELD_NUMBER);
    validateSuccessCodeRanges(healthCheckSettings.getSuccessCodeRangesList());
    if (healthCheckSettings.getInterval().equals(Duration.getDefaultInstance())) {
      throw Status.INVALID_ARGUMENT.withDescription("invalid interval").asRuntimeException();
    }
    if (healthCheckSettings.getTimeout().equals(Duration.getDefaultInstance())) {
      throw Status.INVALID_ARGUMENT.withDescription("invalid timeout").asRuntimeException();
    }
  }

  private void validateSuccessCodeRanges(List<SuccessCodeRange> successCodeRanges) {
    if (successCodeRanges.isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("invalid success code").asRuntimeException();
    }

    for (SuccessCodeRange successCodeRange : successCodeRanges) {
      if (successCodeRange.getStart() < 100
          || successCodeRange.getEnd() >= 600
          || successCodeRange.getStart() > successCodeRange.getEnd()) {
        throw Status.INVALID_ARGUMENT.withDescription("invalid success code").asRuntimeException();
      }
    }
  }

  public List<DeploymentStatus> validateActionAndGetNextStates(
      DeploymentStatus currentStatus, ConfigAccessType accessType, Action action) {
    if (!stateTransitionsRegistry.isActionAllowed(currentStatus, accessType, action)) {
      throw Status.PERMISSION_DENIED
          .withDescription("This operation is not permitted")
          .asRuntimeException();
    }

    return stateTransitionsRegistry.getNextStates(currentStatus, accessType, action);
  }

  public List<DeploymentStatus> validateActionAndGetNextStates(
      DeploymentStatus currentStatus, Action action) {
    if (!stateTransitionsRegistry.isActionAllowed(currentStatus, action)) {
      throw Status.PERMISSION_DENIED
          .withDescription("This operation is not permitted")
          .asRuntimeException();
    }

    return stateTransitionsRegistry.getNextStates(currentStatus, action);
  }

  public void validateUpdatedStatus(
      DeploymentStatus currentStatus,
      ConfigAccessType accessType,
      Action action,
      DeploymentStatus updatedStatus) {
    List<DeploymentStatus> allowedNextStates =
        stateTransitionsRegistry.getNextStates(currentStatus, accessType, action);
    if (!allowedNextStates.contains(updatedStatus)) {
      throw Status.PERMISSION_DENIED
          .withDescription("This operation is not permitted")
          .asRuntimeException();
    }
  }

  private static void validateDomainName(String domain) {
    // Handle wildcard domains
    if (domain.startsWith("*.")) {
      String domainWithoutWildcard = domain.substring(2);
      if (!DOMAIN_VALIDATOR.isValid(domainWithoutWildcard)) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Invalid wildcard domain format: " + domain)
            .asRuntimeException();
      }
    } else if (!DOMAIN_VALIDATOR.isValid(domain)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Invalid domain format: " + domain)
          .asRuntimeException();
    }
  }
}
