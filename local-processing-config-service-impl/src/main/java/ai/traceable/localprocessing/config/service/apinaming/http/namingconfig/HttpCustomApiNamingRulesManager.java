package ai.traceable.localprocessing.config.service.apinaming.http.namingconfig;

import ai.traceable.config.utils.SpanFilterMatcher;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.ServiceIdentifier;
import ai.traceable.span.processing.config.service.v1.ApiNamingRule;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleDetails;
import ai.traceable.span.processing.config.service.v1.GetAllApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
public class HttpCustomApiNamingRulesManager {
  private static final String CACHE_REFRESH_DURATION =
      "api.naming.config.customRules.cache.refreshAfterWriteDuration";
  private static final String CACHE_EXPIRATION_DURATION =
      "api.naming.config.customRules.cache.expireAfterWriteDuration";
  private static final String MAXIMUM_CACHE_SIZE =
      "api.naming.config.customRules.cache.maximumCacheSize";
  private static final String CACHE_NAME = "apiNamingRulesCache";
  private static final Duration CACHE_REFRESH_DURATION_DEFAULT = Duration.ofSeconds(150);
  private static final Duration CACHE_EXPIRATION_DURATION_DEFAULT = Duration.ofSeconds(300);
  private static final long MAXIMUM_CACHE_SIZE_DEFAULT = 1000;

  private final LoadingCache<ContextualKey<ServiceIdentifier>, List<ApiNamingRule>>
      apiNamingRulesCache;
  private final SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      spanProcessingConfigServiceBlockingStub;
  private final SpanFilterMatcher spanFilterMatcher;
  private final ClientConfig clientConfig;

  @Inject
  public HttpCustomApiNamingRulesManager(
      Config config,
      SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
          spanProcessingConfigServiceBlockingStub,
      SpanFilterMatcher spanFilterMatcher,
      ClientConfig clientConfig) {
    this.clientConfig = clientConfig;
    Duration cacheRefreshDuration =
        config.hasPath(CACHE_REFRESH_DURATION)
            ? config.getDuration(CACHE_REFRESH_DURATION)
            : CACHE_REFRESH_DURATION_DEFAULT;
    Duration cacheExpiryDuration =
        config.hasPath(CACHE_EXPIRATION_DURATION)
            ? config.getDuration(CACHE_EXPIRATION_DURATION)
            : CACHE_EXPIRATION_DURATION_DEFAULT;
    long maximumCacheSize =
        config.hasPath(MAXIMUM_CACHE_SIZE)
            ? config.getLong(MAXIMUM_CACHE_SIZE)
            : MAXIMUM_CACHE_SIZE_DEFAULT;

    this.spanProcessingConfigServiceBlockingStub = spanProcessingConfigServiceBlockingStub;
    this.spanFilterMatcher = spanFilterMatcher;
    this.apiNamingRulesCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheRefreshDuration.toMillis(), TimeUnit.MILLISECONDS)
            .expireAfterWrite(cacheExpiryDuration.toMillis(), TimeUnit.MILLISECONDS)
            .maximumSize(maximumCacheSize)
            .recordStats()
            .build(CacheLoader.from(this::loadApiNamingRules));
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        CACHE_NAME, apiNamingRulesCache, Collections.emptyMap(), maximumCacheSize);
  }

  private List<ApiNamingRule> loadApiNamingRules(@Nonnull ContextualKey<ServiceIdentifier> key) {
    try {
      return key
          .callInContext(
              () ->
                  spanProcessingConfigServiceBlockingStub
                      .withDeadlineAfter(
                          clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                      .getAllApiNamingRules(GetAllApiNamingRulesRequest.newBuilder().build()))
          .getRuleDetailsList()
          .stream()
          .map(ApiNamingRuleDetails::getRule)
          .filter(
              apiNamingRule ->
                  !apiNamingRule.getRuleInfo().getDisabled()
                      && spanFilterMatcher.matchesServiceName(
                          apiNamingRule.getRuleInfo().getFilter(), key.getData().getServiceName())
                      && spanFilterMatcher.matchesEnvironment(
                          apiNamingRule.getRuleInfo().getFilter(), key.getData().getEnvironment()))
          .collect(Collectors.toUnmodifiableList());
    } catch (Exception e) {
      log.error(
          "Could not fetch api naming rules for equest context:{}, service identifier:{} with exception:{}",
          key.getContext(),
          key.getData(),
          e);
      return Collections.emptyList();
    }
  }

  public List<ApiNamingRule> getApiNamingRules(
      RequestContext requestContext, String serviceName, Optional<String> environment)
      throws ExecutionException {
    return apiNamingRulesCache.get(
        requestContext.buildInternalContextualKey(new ServiceIdentifier(serviceName, environment)));
  }
}
