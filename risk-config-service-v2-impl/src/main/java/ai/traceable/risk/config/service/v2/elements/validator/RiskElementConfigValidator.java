package ai.traceable.risk.config.service.v2.elements.validator;

import ai.traceable.risk.config.service.v2.RiskElementConfig;
import ai.traceable.risk.config.service.v2.RiskElementConfigUpdates;
import io.grpc.Status;

public interface RiskElementConfigValidator {

  void validateRiskElementUpdateDetails(RiskElementConfigUpdates riskElementConfigUpdates);

  Status validateRiskElementConfig(RiskElementConfig riskElementConfig);
}
