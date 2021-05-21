package ai.traceable.anomaly.config.service.global.status;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusRequest;
import io.grpc.Status;
import javax.inject.Inject;

public class AnomalyGlobalConfigStatusValidator implements ConfigStatusValidator {

  private final AnomalyConfigValidator anomalyConfigValidator;

  @Inject
  public AnomalyGlobalConfigStatusValidator(AnomalyConfigValidator anomalyConfigValidator) {
    this.anomalyConfigValidator = anomalyConfigValidator;
  }

  public Status validate(GetAnomalyGlobalConfigStatusRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Update request should have a valid config scope.");
    }

    return anomalyConfigValidator.validate(request.getConfigScope());
  }

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
}
