package ai.traceable.blocking.config.service.v2.blockingpolicy;

import ai.traceable.blocking.config.service.v2.BlockingPolicyConfiguration;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface BlockingPolicyConfigurationManager {
  BlockingPolicyConfiguration getBlockingPolicyConfiguration(
      RequestContext requestContext, Optional<String> environmentId);
}
