package ai.traceable.anomaly.config.service.apiprotect.validator;

import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextRequest;

public interface ApiProtectValidator {
  void validate(GetApiProtectEvaluationConfigContextRequest request);
}
