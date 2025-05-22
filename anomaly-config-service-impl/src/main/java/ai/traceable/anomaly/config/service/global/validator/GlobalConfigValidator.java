package ai.traceable.anomaly.config.service.global.validator;

import ai.traceable.anomaly.config.service.v1.global.DeleteScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAvailableRuleVersionsRequest;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetUnresolvedScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateScopedAnomalyGlobalConfigStatusRequest;
import io.grpc.Status;

public interface GlobalConfigValidator {

  Status validate(GetScopedAnomalyGlobalConfigStatusRequest request);

  Status validate(GetUnresolvedScopedAnomalyGlobalConfigStatusRequest request);

  Status validate(UpdateScopedAnomalyGlobalConfigStatusRequest request);

  Status validate(GetAnomalyRuleInfosRequest request);

  Status validate(DeleteScopedAnomalyGlobalConfigStatusRequest request);

  Status validate(GetAvailableRuleVersionsRequest request);
}
