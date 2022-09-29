package ai.traceable.anomaly.config.service.override.common.validator;

import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideRuleScope;
import io.grpc.Status;
import lombok.NonNull;

public class DetectionOverrideScopeValidator {
  public Status validateRuleScope(@NonNull DetectionOverrideRuleScope scope) {
    if (scope.hasEnvironmentScope()
        && scope.getEnvironmentScope().getEnvironmentIdsList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Environment scope should have at least one environment");
    } else {
      return Status.OK;
    }
  }
}
