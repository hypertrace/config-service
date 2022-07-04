package ai.traceable.localprocessing.config.service.spanprocessingrules.ratelimitconfig;

import ai.traceable.localprocessing.config.service.v1.RateLimitConfig;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RateLimitConfigManager {
  RateLimitConfig getRateLimitConfig(
      RequestContext requestContext, String serviceName, Optional<String> environment);
}
