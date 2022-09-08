package ai.traceable.anomaly.config.service.global.validator;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.global.DeleteScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosRequest;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetUnresolvedScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateScopedAnomalyGlobalConfigStatusRequest;
import io.grpc.Status;
import javax.inject.Inject;

public class AnomalyGlobalConfigServiceValidator implements GlobalConfigValidator {
  private final AnomalyConfigValidator anomalyConfigValidator;

  @Inject
  public AnomalyGlobalConfigServiceValidator(AnomalyConfigValidator anomalyConfigValidator) {
    this.anomalyConfigValidator = anomalyConfigValidator;
  }

  @Override
  public Status validate(GetScopedAnomalyGlobalConfigStatusRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Update request should have a valid config scope.");
    }
    return anomalyConfigValidator.validate(request.getConfigScope());
  }

  @Override
  public Status validate(GetUnresolvedScopedAnomalyGlobalConfigStatusRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Update request should have a valid config scope.");
    }
    return anomalyConfigValidator.validate(request.getConfigScope());
  }

  @Override
  public Status validate(UpdateScopedAnomalyGlobalConfigStatusRequest request) {
    if (!request.hasScopedConfig()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Update request should have a valid scoped config change object.");
    }
    if (!request.getScopedConfig().hasConfigStatus()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Update request should have a valid config status.");
    }
    if (!request.getScopedConfig().hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Update request should have a valid config scope.");
    }
    Status status;
    if ((status = anomalyConfigValidator.validate(request.getScopedConfig().getConfigStatus()))
        == Status.OK) {
      return anomalyConfigValidator.validate(request.getScopedConfig().getConfigScope());
    }
    return status;
  }

  @Override
  public Status validate(GetAnomalyRuleInfosRequest request) {
    for (AnomalyEventFamily eventFamily : request.getEventFamiliesList()) {
      if (eventFamily == AnomalyEventFamily.ANOMALY_EVENT_FAMILY_UNSPECIFIED) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format("Event family type: %s not defined", eventFamily));
      }
    }
    return Status.OK;
  }

  @Override
  public Status validate(DeleteScopedAnomalyGlobalConfigStatusRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Delete request should have a valid config scope.");
    }
    return anomalyConfigValidator.validate(request.getConfigScope());
  }
}
