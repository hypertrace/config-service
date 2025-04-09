package ai.traceable.threatmanagement.config.service.ipreputation;

import ai.traceable.threatmanagement.config.service.v1.IpReputationThreatScoreConfig;
import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface IpReputationThreatScoreConfigManager {
  IpReputationThreatScoreConfig getIpReputationThreatScoreConfig(
      RequestContext requestContext, ScopeConfig scopeConfig);

  IpReputationThreatScoreConfig updateIpReputationThreatScoreConfig(
      RequestContext requestContext, IpReputationThreatScoreConfig config);

  IpReputationThreatScoreConfig getDefaultIpReputationThreatScoreConfig();
}
