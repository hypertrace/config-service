package ai.traceable.localprocessing.config.service.spanprocessingrules.ratelimitconfig;

import ai.traceable.localprocessing.config.service.v1.RateLimitConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfig;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RateLimitConfigManager {

  List<SamplingConfig> getAllSamplingConfigs(final RequestContext requestContext);

  Optional<RateLimitConfig> getFirstMatchingRateLimitConfig(
      RequestContext requestContext,
      List<SamplingConfig> samplingConfigs,
      String serviceName,
      Optional<String> environment);
}
