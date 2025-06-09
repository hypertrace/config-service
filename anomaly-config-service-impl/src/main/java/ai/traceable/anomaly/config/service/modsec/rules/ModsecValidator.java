package ai.traceable.anomaly.config.service.modsec.rules;

import ai.traceable.anomaly.config.service.v1.modsec.GetDefaultModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetWebAppEvaluationConfigContextRequest;
import io.grpc.Status;

public interface ModsecValidator {
  Status validate(GetModsecCrsRulesRequest request);

  Status validate(GetDefaultModsecCrsRulesRequest request);

  void validate(GetWebAppEvaluationConfigContextRequest request);
}
