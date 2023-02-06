package ai.traceable.blocking.config.service.v1.blockingpolicy;

import ai.traceable.blocking.config.service.v1.BlockingPolicyConfiguration;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface BlockingPolicyConfigurationManager {
  BlockingPolicyConfiguration getBlockingPolicyConfiguration(
      RequestContext requestContext, String requestHash, Optional<String> environmentId);
}
