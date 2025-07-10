package ai.traceable.threatmanagement.config.service.threatscore;

import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreDecay;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ThreatScoreDecayManager {
  ThreatScoreDecay getThreatScoreDecay(RequestContext requestContext, ScopeConfig scopeConfig);

  ThreatScoreDecay upsertThreatScoreDecay(
      RequestContext requestContext, ThreatScoreDecay threatScoreDecay);

  ThreatScoreDecay getDefaultThreatScoreDecay();
}
