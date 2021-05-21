package ai.traceable.anomaly.config.service.global.status;

import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusRequest;
import io.grpc.Status;

public interface ConfigStatusValidator {

  Status validate(UpdateAnomalyGlobalConfigStatusRequest request);

  Status validate(GetAnomalyGlobalConfigStatusRequest request);
}
