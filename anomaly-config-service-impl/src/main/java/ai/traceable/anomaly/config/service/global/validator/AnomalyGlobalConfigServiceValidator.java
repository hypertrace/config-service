package ai.traceable.anomaly.config.service.global.validator;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusRequest;
import io.grpc.Status;
import javax.inject.Inject;

public class AnomalyGlobalConfigServiceValidator implements GlobalConfigValidator {
  private final AnomalyConfigValidator anomalyConfigValidator;

  @Inject
  public AnomalyGlobalConfigServiceValidator(AnomalyConfigValidator anomalyConfigValidator) {
    this.anomalyConfigValidator = anomalyConfigValidator;
  }

  @Override
  public Status validate(GetAnomalyGlobalConfigStatusRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Update request should have a valid config scope.");
    }
    return anomalyConfigValidator.validate(request.getConfigScope());
  }

  @Override
  public Status validate(UpdateAnomalyGlobalConfigStatusRequest request) {
    if (!request.hasConfigStatus()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Update request should have a valid config status.");
    }
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Update request should have a valid config scope.");
    }
    Status status;
    if ((status = anomalyConfigValidator.validate(request.getConfigStatus())) == Status.OK) {
      return anomalyConfigValidator.validate(request.getConfigScope());
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
}
