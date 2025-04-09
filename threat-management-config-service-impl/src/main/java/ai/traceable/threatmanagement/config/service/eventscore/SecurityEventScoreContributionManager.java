package ai.traceable.threatmanagement.config.service.eventscore;

import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface SecurityEventScoreContributionManager {
  // get security event score contribution, if present, else return default security event score
  // contribution
  SecurityEventScoreContribution getSecurityEventScoreContribution(
      RequestContext requestContext, ScopeConfig config);

  // update if config present, else create
  SecurityEventScoreContribution upsertSecurityEventScoreContribution(
      RequestContext requestContext, SecurityEventScoreContribution securityEventScoreContribution);

  SecurityEventScoreContribution getDefaultSecurityEventScoreContribution();
}
