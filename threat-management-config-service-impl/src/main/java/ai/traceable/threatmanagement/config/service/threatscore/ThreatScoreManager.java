package ai.traceable.threatmanagement.config.service.threatscore;

import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ThreatScoreManager {
  // get threat score bound, if present, else return default score bound
  ThreatScoreBound getThreatScoreBound(RequestContext requestContext, ScopeConfig scopeConfig);

  // update if config present, else create
  ThreatScoreBound upsertThreatScoreBound(
      RequestContext requestContext, ThreatScoreBound threatScoreBound);

  ThreatScoreBound getDefaultThreatScoreBound();
}
