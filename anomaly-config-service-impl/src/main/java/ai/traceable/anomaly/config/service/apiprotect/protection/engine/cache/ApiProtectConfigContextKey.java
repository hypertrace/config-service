package ai.traceable.anomaly.config.service.apiprotect.protection.engine.cache;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.RuleEvaluationPoint;
import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextRequest;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode
public class ApiProtectConfigContextKey {
  RuleEvaluationPoint ruleEvaluationPoint;
  AnomalyConfigScope configScope;

  static ApiProtectConfigContextKey from(GetApiProtectEvaluationConfigContextRequest request) {
    return new ApiProtectConfigContextKey(
        request.getRuleEvaluationPoint(), request.getConfigScope());
  }

  GetApiProtectEvaluationConfigContextRequest getRequest() {
    return GetApiProtectEvaluationConfigContextRequest.newBuilder()
        .setRuleEvaluationPoint(ruleEvaluationPoint)
        .setConfigScope(configScope)
        .build();
  }
}
