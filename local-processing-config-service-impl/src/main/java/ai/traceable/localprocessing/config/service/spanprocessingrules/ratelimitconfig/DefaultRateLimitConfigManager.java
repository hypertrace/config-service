package ai.traceable.localprocessing.config.service.spanprocessingrules.ratelimitconfig;

import static ai.traceable.span.processing.config.service.v1.RateLimitStrategy.RATE_LIMIT_STRATEGY_BARESPAN;
import static ai.traceable.span.processing.config.service.v1.RateLimitStrategy.RATE_LIMIT_STRATEGY_DROP;

import ai.traceable.config.utils.SpanFilterMatcher;
import ai.traceable.localprocessing.config.service.v1.RateLimit;
import ai.traceable.localprocessing.config.service.v1.RateLimitConfig;
import ai.traceable.localprocessing.config.service.v1.RateLimitStrategy;
import ai.traceable.localprocessing.config.service.v1.WindowedRateLimit;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.SamplingConfig;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import com.google.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DefaultRateLimitConfigManager implements RateLimitConfigManager {

  // This set dictates the sampling configs with rate limiting strategies which are allowed to be
  // passed to the agent
  private static final Set<ai.traceable.span.processing.config.service.v1.RateLimitStrategy>
      ALLOWED_RATE_LIMIT_STRATEGIES =
          new HashSet<>(Set.of(RATE_LIMIT_STRATEGY_BARESPAN, RATE_LIMIT_STRATEGY_DROP));

  private final SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      configServiceBlockingStub;
  private final SpanFilterMatcher spanFilterMatcher;

  @Inject
  public DefaultRateLimitConfigManager(
      SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
          configServiceBlockingStub,
      SpanFilterMatcher spanFilterMatcher) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.spanFilterMatcher = spanFilterMatcher;
  }

  @Override
  public List<SamplingConfig> getAllSamplingConfigs(final RequestContext requestContext) {
    return requestContext
        .call(
            () ->
                configServiceBlockingStub.getAllResolvedSamplingConfigs(
                    GetAllResolvedSamplingConfigsRequest.newBuilder().build()))
        .getSamplingConfigsList();
  }

  @Override
  public Optional<RateLimitConfig> getFirstMatchingRateLimitConfig(
      RequestContext requestContext,
      List<SamplingConfig> samplingConfigs,
      String serviceName,
      Optional<String> environment) {
    return samplingConfigs.stream()
        .map(samplingConfig -> convertSamplingConfig(samplingConfig, serviceName, environment))
        .filter(Optional::isPresent)
        .findFirst()
        .map(Optional::get);
  }

  private Optional<RateLimitConfig> convertSamplingConfig(
      ai.traceable.span.processing.config.service.v1.SamplingConfig samplingConfig,
      String serviceName,
      Optional<String> environment) {

    ai.traceable.span.processing.config.service.v1.RateLimitConfig rateLimitConfig =
        samplingConfig.getSamplingConfigInfo().getRateLimitConfig();
    if (!ALLOWED_RATE_LIMIT_STRATEGIES.contains(rateLimitConfig.getRateLimitStrategy())) {
      return Optional.empty();
    }

    // apply environment filters if any
    if (!spanFilterMatcher.matchesEnvironment(
        samplingConfig.getSamplingConfigInfo().getFilter(), environment)) {
      return Optional.empty();
    }

    // apply service name filters if any
    if (!spanFilterMatcher.matchesServiceName(
        samplingConfig.getSamplingConfigInfo().getFilter(), serviceName)) {
      return Optional.empty();
    }

    return Optional.of(convertRateLimitConfig(rateLimitConfig));
  }

  private RateLimitConfig convertRateLimitConfig(
      ai.traceable.span.processing.config.service.v1.RateLimitConfig rateLimitConfig) {
    return RateLimitConfig.newBuilder()
        .setApiEndpointCacheDuration(rateLimitConfig.getApiEndpointCacheDuration())
        .setTraceLimitGlobal(convertRateLimit(rateLimitConfig.getTraceLimitGlobal()))
        .setTraceLimitPerEndpoint(convertRateLimit(rateLimitConfig.getTraceLimitPerEndpoint()))
        .setRateLimitStrategy(convertRateLimitStrategy(rateLimitConfig.getRateLimitStrategy()))
        .build();
  }

  private RateLimitStrategy convertRateLimitStrategy(
      ai.traceable.span.processing.config.service.v1.RateLimitStrategy rateLimitStrategy) {
    switch (rateLimitStrategy) {
      case RATE_LIMIT_STRATEGY_DROP:
        return RateLimitStrategy.RATE_LIMIT_STRATEGY_DROP;
      case RATE_LIMIT_STRATEGY_BARESPAN:
        return RateLimitStrategy.RATE_LIMIT_STRATEGY_BARESPAN;
      default:
        throw new UnsupportedOperationException(
            "Unknown rate limit strategy: " + rateLimitStrategy);
    }
  }

  private RateLimit convertRateLimit(
      ai.traceable.span.processing.config.service.v1.RateLimit rateLimit) {
    switch (rateLimit.getLimitCase()) {
      case FIXED_WINDOW_LIMIT:
        return RateLimit.newBuilder()
            .setFixedWindowLimit(convertWindowedRateLimit(rateLimit.getFixedWindowLimit()))
            .build();
      default:
        throw new UnsupportedOperationException("unknown rate limit type: " + rateLimit);
    }
  }

  private WindowedRateLimit convertWindowedRateLimit(
      ai.traceable.span.processing.config.service.v1.WindowedRateLimit windowedRateLimit) {
    return WindowedRateLimit.newBuilder()
        .setQuantityAllowed(windowedRateLimit.getQuantityAllowed())
        .setWindowDuration(windowedRateLimit.getWindowDuration())
        .build();
  }
}
