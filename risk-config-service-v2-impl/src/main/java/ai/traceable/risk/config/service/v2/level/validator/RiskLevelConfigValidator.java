package ai.traceable.risk.config.service.v2.level.validator;

import ai.traceable.risk.config.service.v2.RiskLevelConfigValues;
import io.grpc.Status;

public interface RiskLevelConfigValidator {

  Status validateRiskLevelConfigValues(RiskLevelConfigValues values);
}
