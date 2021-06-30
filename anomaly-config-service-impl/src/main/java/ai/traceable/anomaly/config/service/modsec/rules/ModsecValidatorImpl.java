package ai.traceable.anomaly.config.service.modsec.rules;

import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesType;
import io.grpc.Status;

public class ModsecValidatorImpl implements ModsecValidator {
  @Override
  public Status validate(GetModsecCrsRulesRequest request) {
    for (ModsecCrsRulesType type : request.getModsecCrsRulesTypesList()) {
      if (type == ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_UNSPECIFIED) {
        return Status.INVALID_ARGUMENT.withDescription("Modsec rule should have a valid type");
      }
    }
    return Status.OK;
  }
}
