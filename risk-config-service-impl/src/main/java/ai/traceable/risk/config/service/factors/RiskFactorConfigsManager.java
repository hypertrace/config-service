package ai.traceable.risk.config.service.factors;

import ai.traceable.risk.config.service.v1.RiskContributorConfigs;
import ai.traceable.risk.config.service.v1.RiskContributorConfigsResetFilter;
import ai.traceable.risk.config.service.v1.RiskFactorConfig;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RiskFactorConfigsManager {
  String LIKELIHOOD_ANNOTATION = "Likelihood";
  String IMPACT_ANNOTATION = "Impact";

  RiskContributorConfigs getRiskLikelihoodConfigs(RequestContext requestContext);

  RiskContributorConfigs updateRiskLikelihoodConfigs(
      RequestContext requestContext, List<RiskFactorConfig> riskFactorConfigs);

  RiskContributorConfigs resetRiskLikelihoodConfigs(
      RequestContext requestContext, RiskContributorConfigsResetFilter filter);

  RiskContributorConfigs getRiskImpactConfigs(RequestContext requestContext);

  RiskContributorConfigs updateRiskImpactConfigs(
      RequestContext requestContext, List<RiskFactorConfig> riskFactorConfigs);

  RiskContributorConfigs resetRiskImpactConfigs(
      RequestContext requestContext, RiskContributorConfigsResetFilter filter);
}
