package ai.traceable.aiapp.protection.config.service.firewall.cache;

import ai.traceable.aiapp.protection.config.service.v1.GetAiAppEvaluationConfigContextRequest;
import ai.traceable.aiapp.protection.config.service.v1.RuleEvaluationPoint;
import ai.traceable.aiapp.protection.config.service.v1.RuleScope;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode
public class AiAppConfigContextKey {
  RuleEvaluationPoint ruleEvaluationPoint;
  RuleScope ruleScope;

  static AiAppConfigContextKey from(GetAiAppEvaluationConfigContextRequest request) {
    return new AiAppConfigContextKey(request.getRuleEvaluationPoint(), request.getRuleScope());
  }

  GetAiAppEvaluationConfigContextRequest getRequest() {
    return GetAiAppEvaluationConfigContextRequest.newBuilder()
        .setRuleEvaluationPoint(ruleEvaluationPoint)
        .setRuleScope(ruleScope)
        .build();
  }
}
