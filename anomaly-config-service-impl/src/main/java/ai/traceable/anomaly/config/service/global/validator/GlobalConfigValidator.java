package ai.traceable.anomaly.config.service.global.validator;

import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusRequest;
import io.grpc.Status;

public interface GlobalConfigValidator {
  Status validate(UpdateAnomalyGlobalConfigStatusRequest request);

  Status validate(GetAnomalyGlobalConfigStatusRequest request);

  Status validate(GetAnomalyRuleInfosRequest request);
}
