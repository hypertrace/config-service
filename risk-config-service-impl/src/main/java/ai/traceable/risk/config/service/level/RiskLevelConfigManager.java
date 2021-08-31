package ai.traceable.risk.config.service.level;

import ai.traceable.risk.config.service.v1.RiskLevelConfig;
import ai.traceable.risk.config.service.v1.RiskLevelConfigValues;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RiskLevelConfigManager {

  RiskLevelConfig getRiskLevelConfig(RequestContext requestContext);

  RiskLevelConfig updateRiskLevelConfig(
      RequestContext requestContext, RiskLevelConfigValues riskLevelConfigValues);

  RiskLevelConfig resetRiskLevelConfig(RequestContext requestContext);
}
