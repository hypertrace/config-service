package ai.traceable.threatmanagement.config.service.statuscode;

import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import ai.traceable.threatmanagement.config.service.v1.StatusCodeThreatScoreConfigs;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface StatusCodeThreatScoreConfigsManager {
  StatusCodeThreatScoreConfigs getStatusCodeThreatScoreConfigs(
      RequestContext requestContext, ScopeConfig scopeConfig);

  StatusCodeThreatScoreConfigs updateStatusCodeThreatScoreConfigs(
      RequestContext requestContext, StatusCodeThreatScoreConfigs configs);
}
