package ai.traceable.blocking.config.service.blockingpolicy;

import ai.traceable.blocking.config.service.v1.BlockingPolicyConfiguration;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface BlockingPolicyConfigurationManager {
  BlockingPolicyConfiguration getBlockingPolicyConfiguration(
      RequestContext requestContext, String requestHash);
}
