package ai.traceable.risk.config.service.v2.grid.manager;

import ai.traceable.risk.config.service.v2.RiskConfigScope;
import ai.traceable.risk.config.service.v2.RiskScoringGridCell;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfig;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RiskScoringGridConfigManager {

  RiskScoringGridConfig getRiskScoringGridConfig(
      RequestContext requestContext, RiskConfigScope riskConfigScope);

  RiskScoringGridConfig updateRiskScoringGridConfig(
      RequestContext requestContext,
      RiskConfigScope riskConfigScope,
      List<RiskScoringGridCell> riskScoringGridCells);

  RiskScoringGridConfig resetRiskScoringGridConfig(
      RequestContext requestContext, RiskConfigScope riskConfigScope);
}
