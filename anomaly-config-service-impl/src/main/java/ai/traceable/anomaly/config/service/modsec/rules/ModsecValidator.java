package ai.traceable.anomaly.config.service.modsec.rules;

import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import io.grpc.Status;

public interface ModsecValidator {
  Status validate(GetModsecCrsRulesRequest request);
}
