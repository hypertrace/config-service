package ai.traceable.threatmanagement.config.service.eventtype;

import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface SecurityEventTypeContributionManager {
  // get security event type contribution, if present, else return default security event type
  // contribution
  SecurityEventTypeContribution getSecurityEventTypeContribution(
      RequestContext requestContext, ScopeConfig scopeConfig);

  // update if config present, else create
  SecurityEventTypeContribution upsertSecurityEventTypeContribution(
      RequestContext requestContext, SecurityEventTypeContribution securityEventTypeContribution);

  SecurityEventTypeContribution getDefaultSecurityEventTypeContribution();
}
