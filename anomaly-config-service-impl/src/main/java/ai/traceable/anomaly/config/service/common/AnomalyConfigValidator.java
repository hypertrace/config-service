package ai.traceable.anomaly.config.service.common;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScopeType;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import io.grpc.Status;

public class AnomalyConfigValidator {

  public Status validate(AnomalyConfigScope configScope) {
    if (configScope.getScopeType()
        == AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Scope should have a valid scope type.");
    }
    if (configScope.getScopeType() == AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_SERVICE
        && configScope.getServiceScope().getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Scope SERVICE should have valid Service Scope.");
    }
    if (configScope.getScopeType() == AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_API
        && (configScope.getApiScope().getId().isEmpty()
            || configScope.getApiScope().getServiceScope().getId().isEmpty())) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Scope API should have valid API Scope.");
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
