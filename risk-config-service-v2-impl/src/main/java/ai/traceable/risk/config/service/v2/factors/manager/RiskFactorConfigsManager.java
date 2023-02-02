package ai.traceable.risk.config.service.v2.factors.manager;

import ai.traceable.risk.config.service.v2.RiskConfigScope;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.RiskFactorCategory;
import ai.traceable.risk.config.service.v2.RiskFactorConfigUpdateDetails;
import java.util.Collection;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RiskFactorConfigsManager {

  RiskContributorConfigs getRiskContributorConfigs(
      RequestContext requestContext, RiskConfigScope riskConfigScope);

  RiskContributorConfigs updateRiskContributorConfigs(
      RequestContext requestContext,
      Collection<RiskFactorConfigUpdateDetails> riskFactorConfigUpdateDetailsList,
      RiskConfigScope riskConfigScope);

  RiskContributorConfigs resetRiskContributorConfigs(
      RequestContext requestContext,
      Collection<RiskFactorCategory> riskFactorCategoriesList,
      RiskConfigScope riskConfigScope);
}
