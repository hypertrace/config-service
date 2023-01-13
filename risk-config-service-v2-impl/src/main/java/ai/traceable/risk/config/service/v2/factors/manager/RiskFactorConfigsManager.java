package ai.traceable.risk.config.service.v2.factors.manager;

import ai.traceable.risk.config.service.v2.RiskConfigScope;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.RiskFactorCategory;
import ai.traceable.risk.config.service.v2.RiskFactorConfigUpdateDetails;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RiskFactorConfigsManager {

  RiskContributorConfigs getRiskContributorConfigs(
      RequestContext requestContext, RiskConfigScope riskConfigScope);

  RiskContributorConfigs updateRiskContributorConfigs(
      RequestContext requestContext,
      List<RiskFactorConfigUpdateDetails> riskFactorConfigUpdateDetailsList,
      RiskConfigScope riskConfigScope);

  RiskContributorConfigs resetRiskContributorConfigs(
      RequestContext requestContext,
      List<RiskFactorCategory> riskFactorCategoriesList,
      RiskConfigScope riskConfigScope);
}
