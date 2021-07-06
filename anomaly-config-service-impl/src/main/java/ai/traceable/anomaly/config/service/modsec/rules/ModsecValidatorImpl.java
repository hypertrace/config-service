package ai.traceable.anomaly.config.service.modsec.rules;

import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import io.grpc.Status;

public class ModsecValidatorImpl implements ModsecValidator {
  @Override
  public Status validate(GetModsecCrsRulesRequest request) {
    for (AnomalySubRuleType type : request.getSubRuleTypesList()) {
      if (type == AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_UNSPECIFIED) {
        return Status.INVALID_ARGUMENT.withDescription("Modsec rule should have a valid type");
      }
    }
    return Status.OK;
  }
}
