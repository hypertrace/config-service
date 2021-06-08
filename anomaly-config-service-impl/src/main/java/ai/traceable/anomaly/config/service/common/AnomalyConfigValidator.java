package ai.traceable.anomaly.config.service.common;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import io.grpc.Status;

public class AnomalyConfigValidator {

  public Status validate(AnomalyConfigScope configScope) {
    if (configScope.getScopeCase() == AnomalyConfigScope.ScopeCase.SERVICE_SCOPE
        && configScope.getServiceScope().getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config SERVICE Scope should have valid Service ID.");
    }
    if (configScope.getScopeCase() == AnomalyConfigScope.ScopeCase.API_SCOPE
        && (configScope.getApiScope().getId().isEmpty()
            || configScope.getApiScope().getServiceScope().getId().isEmpty())) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config API Scope should have valid API and Service IDs.");
    }
    if (configScope.getScopeCase() == AnomalyConfigScope.ScopeCase.PARAM_SCOPE
        && (configScope.getParamScope().getApiScope().getId().isEmpty()
            || configScope.getParamScope().getApiScope().getServiceScope().getId().isEmpty()
            || configScope.getParamScope().getParamName().isEmpty())) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config PARAM Scope should have a valid API and Service IDs and valid param name.");
    }
    return Status.OK;
  }

  public Status validate(AnomalyConfigStatusChange configStatusChange) {
    if (!configStatusChange.hasDisabled() && !configStatusChange.hasInternal()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Status Change should have at least one of disabled and internal values");
    }
    return Status.OK;
  }
}
