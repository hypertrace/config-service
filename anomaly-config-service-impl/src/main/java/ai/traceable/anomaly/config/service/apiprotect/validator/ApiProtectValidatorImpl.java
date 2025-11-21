package ai.traceable.anomaly.config.service.apiprotect.validator;

import ai.traceable.anomaly.config.service.v1.RuleEvaluationPoint;
import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextRequest;
import com.google.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class ApiProtectValidatorImpl implements ApiProtectValidator {

  @Override
  public void validate(GetApiProtectEvaluationConfigContextRequest request) {
    if (request.getRuleEvaluationPoint() == RuleEvaluationPoint.RULE_EVALUATION_POINT_UNSPECIFIED) {
      throw new IllegalArgumentException("Rule evaluation point must be specified");
    }
  }
}
