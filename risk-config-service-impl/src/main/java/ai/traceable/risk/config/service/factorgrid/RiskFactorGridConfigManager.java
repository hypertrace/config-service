package ai.traceable.risk.config.service.factorgrid;

import ai.traceable.risk.config.service.v1.RiskFactorGridConfig;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfigValues;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RiskFactorGridConfigManager {

  RiskFactorGridConfig getRiskFactorGridConfig(RequestContext requestContext);

  RiskFactorGridConfig updateRiskFactorGridConfig(
      RequestContext requestContext, RiskFactorGridConfigValues riskFactorGridConfigValues);

  RiskFactorGridConfig resetRiskFactorGridConfig(RequestContext requestContext);
}
