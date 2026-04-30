package ai.traceable.localprocessing.config.service.spanprocessingrules.ratelimitconfig;

import static ai.traceable.span.processing.config.service.v1.RateLimitStrategy.RATE_LIMIT_STRATEGY_BARESPAN;
import static ai.traceable.span.processing.config.service.v1.RateLimitStrategy.RATE_LIMIT_STRATEGY_DROP;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.SpanFilterMatcher;
import ai.traceable.localprocessing.config.service.utils.FilterConverter;
import ai.traceable.localprocessing.config.service.v1.PercentageLimitConfig;
import ai.traceable.localprocessing.config.service.v1.RateLimit;
import ai.traceable.localprocessing.config.service.v1.RateLimitConfig;
import ai.traceable.localprocessing.config.service.v1.RateLimitStrategy;
import ai.traceable.localprocessing.config.service.v1.SpanFilter;
import ai.traceable.localprocessing.config.service.v1.SpanLimitingStrategy;
import ai.traceable.localprocessing.config.service.v1.WindowedRateLimit;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.SamplingConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfigInfo;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import com.google.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ClientConfig;
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
  private final ClientConfig clientConfig;
  private final FilterConverter filterConverter;
  private final FeatureCachingClient featureCachingClient;

  @Inject
  public DefaultRateLimitConfigManager(
      SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
          configServiceBlockingStub,
      SpanFilterMatcher spanFilterMatcher,
      ClientConfig clientConfig,
      FilterConverter filterConverter,
      FeatureCachingClient featureCachingClient) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.spanFilterMatcher = spanFilterMatcher;
    this.clientConfig = clientConfig;
    this.filterConverter = filterConverter;
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  public List<SamplingConfig> getAllSamplingConfigs(final RequestContext requestContext) {
    return requestContext
        .call(
            () ->
                configServiceBlockingStub
                    .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getAllResolvedSamplingConfigs(
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
        .flatMap(Optional::stream)
        .findFirst();
  }

  @Override
  public List<RateLimitConfig> getAllCustomRateLimitConfigs(
      RequestContext requestContext,
      List<SamplingConfig> samplingConfigs,
      String serviceName,
      Optional<String> environment) {
    // Check if custom rate limit config is disabled
    if (!featureCachingClient.isTpaCustomRateLimitConfigEnabled(requestContext)) {
      return List.of();
    }
    return samplingConfigs.stream()
        .map(
            samplingConfig ->
                convertToCustomRateLimitConfig(samplingConfig, serviceName, environment))
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
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

    ai.traceable.span.processing.config.service.v1.SpanFilter spanFilter =
        samplingConfig.getSamplingConfigInfo().getFilter();

    /**
     * If the sampling config has any span filters other than environment and service name filters,
     * not passing anything to the agent as the matched sampling config might not be correct given
     * that we are considering only service and environment filters ignoring the rest. Leaving these
     * out allows for them to get applied on the platform which will ensure correctness
     */
    if (!spanFilterMatcher.hasOnlyEnvironmentAndServiceNameFilters(spanFilter)) {
      return Optional.empty();
    }

    // apply environment filters if any
    if (!spanFilterMatcher.matchesEnvironment(spanFilter, environment)) {
      return Optional.empty();
    }

    // apply service name filters if any
    if (!spanFilterMatcher.matchesServiceName(spanFilter, serviceName)) {
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

  @Override
  public List<PercentageLimitConfig> getAllMatchingPercentageLimitConfigs(
      RequestContext requestContext,
      List<SamplingConfig> samplingConfigs,
      String serviceName,
      Optional<String> environment) {
    return samplingConfigs.stream()
        .map(
            samplingConfig ->
                convertToPercentageLimitConfig(samplingConfig, serviceName, environment))
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<PercentageLimitConfig> convertToPercentageLimitConfig(
      SamplingConfig samplingConfig, String serviceName, Optional<String> environment) {
    if (samplingConfig.getId().isEmpty()) {
      return Optional.empty();
    }

    SamplingConfigInfo samplingConfigInfo = samplingConfig.getSamplingConfigInfo();

    if (!samplingConfigInfo.hasPercentageLimitConfig()) {
      return Optional.empty();
    }

    ai.traceable.span.processing.config.service.v1.SpanFilter spanFilter =
        samplingConfigInfo.getFilter();

    // apply environment filters if any
    if (!spanFilterMatcher.matchesEnvironment(spanFilter, environment)) {
      return Optional.empty();
    }

    // apply service name filters if any
    if (!spanFilterMatcher.matchesServiceName(spanFilter, serviceName)) {
      return Optional.empty();
    }

    ai.traceable.span.processing.config.service.v1.PercentageLimitConfig sourceConfig =
        samplingConfigInfo.getPercentageLimitConfig();

    Optional<SpanFilter> filter = filterConverter.convert(spanFilter);

    // Environment and service-name clauses are consumed at routing time (the agent already queries
    // for a specific env/service) and are stripped during conversion. If that leaves no per-span
    // filter, the config has nothing meaningful to evaluate on the agent, so don't return it.
    if (filter.isEmpty()) {
      return Optional.empty();
    }

    PercentageLimitConfig.Builder builder =
        PercentageLimitConfig.newBuilder()
            .setId(samplingConfig.getId())
            .setAllowedPercentage(sourceConfig.getAllowedPercentage())
            .setLimitingStrategy(convertSpanLimitingStrategy(sourceConfig.getLimitingStrategy()))
            .setFilter(filter.get());

    return Optional.of(builder.build());
  }

  private SpanLimitingStrategy convertSpanLimitingStrategy(
      ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy spanLimitingStrategy) {
    switch (spanLimitingStrategy) {
      case SPAN_LIMITING_STRATEGY_DROP:
        return SpanLimitingStrategy.SPAN_LIMITING_STRATEGY_DROP;
      case SPAN_LIMITING_STRATEGY_BARESPAN:
        return SpanLimitingStrategy.SPAN_LIMITING_STRATEGY_BARESPAN;
      default:
        throw new UnsupportedOperationException(
            "Unknown span limiting strategy: " + spanLimitingStrategy);
    }
  }

  private Optional<RateLimitConfig> convertToCustomRateLimitConfig(
      SamplingConfig samplingConfig, String serviceName, Optional<String> environment) {
    // check if the config has an ID
    if (samplingConfig.getId().isEmpty()) {
      return Optional.empty();
    }

    ai.traceable.span.processing.config.service.v1.SamplingConfigInfo samplingConfigInfo =
        samplingConfig.getSamplingConfigInfo();
    ai.traceable.span.processing.config.service.v1.RateLimitConfig rateLimitConfig =
        samplingConfigInfo.getRateLimitConfig();
    ai.traceable.span.processing.config.service.v1.SpanFilter spanFilter =
        samplingConfigInfo.getFilter();

    // check if rate limit strategy is allowed
    if (!ALLOWED_RATE_LIMIT_STRATEGIES.contains(rateLimitConfig.getRateLimitStrategy())) {
      return Optional.empty();
    }

    // apply environment filters if any
    if (!spanFilterMatcher.matchesEnvironment(spanFilter, environment)) {
      return Optional.empty();
    }

    // apply service name filters if any
    if (!spanFilterMatcher.matchesServiceName(spanFilter, serviceName)) {
      return Optional.empty();
    }

    Optional<SpanFilter> filter = filterConverter.convert(spanFilter);

    // Environment and service-name clauses are consumed at routing time (the agent already queries
    // for a specific env/service) and are stripped during conversion. If that leaves no per-span
    // filter, the config has nothing meaningful to evaluate on the agent, so don't return it.
    if (filter.isEmpty()) {
      return Optional.empty();
    }

    RateLimitConfig.Builder builder =
        RateLimitConfig.newBuilder()
            .setApiEndpointCacheDuration(rateLimitConfig.getApiEndpointCacheDuration())
            .setTraceLimitGlobal(convertRateLimit(rateLimitConfig.getTraceLimitGlobal()))
            .setTraceLimitPerEndpoint(convertRateLimit(rateLimitConfig.getTraceLimitPerEndpoint()))
            .setRateLimitStrategy(convertRateLimitStrategy(rateLimitConfig.getRateLimitStrategy()))
            .setId(samplingConfig.getId())
            .setFilter(filter.get());

    return Optional.of(builder.build());
  }
}
