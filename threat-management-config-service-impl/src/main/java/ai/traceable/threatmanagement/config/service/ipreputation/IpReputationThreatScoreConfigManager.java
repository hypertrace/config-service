package ai.traceable.threatmanagement.config.service.ipreputation;

import ai.traceable.threatmanagement.config.service.v1.IpReputationThreatScoreConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface IpReputationThreatScoreConfigManager {
  IpReputationThreatScoreConfig getIpReputationThreatScoreConfig(RequestContext requestContext);

  IpReputationThreatScoreConfig updateIpReputationThreatScoreConfig(
      RequestContext requestContext, IpReputationThreatScoreConfig config);

  IpReputationThreatScoreConfig getDefaultIpReputationThreatScoreConfig();
}
