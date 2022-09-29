package ai.traceable.anomaly.config.service.override.common.validator;

import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideActorsConfig;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideAssetConfig;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideEntityScope;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideScope;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideScopeConfig;
import io.grpc.Status;
import javax.inject.Inject;
import lombok.NonNull;

public class DetectionOverrideConfigsValidator {

  private final DetectionOverrideConditionsValidator conditionsValidator;

  @Inject
  public DetectionOverrideConfigsValidator(
      DetectionOverrideConditionsValidator conditionsValidator) {
    this.conditionsValidator = conditionsValidator;
  }

  public Status validateActorsConfig(@NonNull DetectionOverrideActorsConfig config) {
    switch (config.getConfigCase()) {
      case ACTOR_ENTITY_IDS:
        if (config.getActorEntityIds().getValuesList().isEmpty()) {
          return Status.INVALID_ARGUMENT.withDescription(
              "Actor Entity Ids should have at least one id");
        }
        return Status.OK;
      default:
        return Status.INVALID_ARGUMENT.withDescription(
            String.format("Invalid case of actors config %s", config.getConfigCase()));
    }
  }

  public Status validateAssetConfig(@NonNull DetectionOverrideAssetConfig config) {
    if (!config.hasScopeConfig() && !config.hasConditionsConfig()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Asset config should have a valid scope config or a valid conditions config.");
    }

    Status status;
    if (config.hasScopeConfig()
        && (status = validateScopeConfig(config.getScopeConfig())) != Status.OK) {
      return status;
    }
    if (config.hasConditionsConfig()
        && (status = conditionsValidator.validateConditionsConfig(config.getConditionsConfig()))
            != Status.OK) {
      return status;
    }
    return Status.OK;
  }

  public Status validateScopeConfig(@NonNull DetectionOverrideScopeConfig config) {
    if (config.getScopesList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Scope config should have at least one scope");
    }
    Status status;
    for (DetectionOverrideScope scope : config.getScopesList()) {
      if ((status = validateScope(scope)) != Status.OK) {
        return status;
      }
    }
    return Status.OK;
  }

  private Status validateScope(@NonNull DetectionOverrideScope scope) {
    switch (scope.getScopeCase()) {
      case API_SCOPE:
        DetectionOverrideEntityScope apiScope = scope.getApiScope();
        if (apiScope.getEntityIds().getValuesList().isEmpty()
            && apiScope.getLabelIds().getValuesList().isEmpty()) {
          return Status.INVALID_ARGUMENT.withDescription(
              "Api Ids and api labels both shouldn't be empty");
        }
        return Status.OK;
      case SERVICE_SCOPE:
        DetectionOverrideEntityScope serviceScope = scope.getServiceScope();
        if (serviceScope.getEntityIds().getValuesList().isEmpty()
            && serviceScope.getLabelIds().getValuesList().isEmpty()) {
          return Status.INVALID_ARGUMENT.withDescription(
              "Service Ids and service labels both shouldn't be empty");
        }
        return Status.OK;
      default:
        return Status.INVALID_ARGUMENT.withDescription(
            String.format("Invalid case of scope %s", scope.getScopeCase()));
    }
  }
}
