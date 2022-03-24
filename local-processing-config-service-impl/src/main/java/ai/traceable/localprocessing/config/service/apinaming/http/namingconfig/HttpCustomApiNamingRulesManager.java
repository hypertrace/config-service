package ai.traceable.localprocessing.config.service.apinaming.http.namingconfig;

import ai.traceable.localprocessing.config.service.apinaming.http.utils.ServiceIdentifier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.utils.SpanFilterMatcher;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleDetails;
import org.hypertrace.span.processing.config.service.v1.GetAllApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;

@Slf4j
public class HttpCustomApiNamingRulesManager {
  private static final String CACHE_REFRESH_DURATION =
      "api.naming.config.customRules.cache.refreshAfterWriteDuration";
  private static final String CACHE_EXPIRATION_DURATION =
      "api.naming.config.customRules.cache.expireAfterWriteDuration";
  private static final String MAXIMUM_CACHE_SIZE =
      "api.naming.config.customRules.cache.maximumCacheSize";
  private static final String CACHE_NAME = "apiNamingRulesCache";
  private static final Duration CACHE_REFRESH_DURATION_DEFAULT = Duration.ofHours(12);
  private static final Duration CACHE_EXPIRATION_DURATION_DEFAULT = Duration.ofHours(24);
  private static final long MAXIMUM_CACHE_SIZE_DEFAULT = 100;

  private final LoadingCache<ContextualKey<ServiceIdentifier>, List<ApiNamingRule>>
      apiNamingRulesCache;
  private final SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      spanProcessingConfigServiceBlockingStub;
  private final SpanFilterMatcher spanFilterMatcher;

  @Inject
  public HttpCustomApiNamingRulesManager(
      Config config,
      SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
          spanProcessingConfigServiceBlockingStub,
      SpanFilterMatcher spanFilterMatcher) {
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
    PlatformMetricsRegistry.registerCache(CACHE_NAME, apiNamingRulesCache, Collections.emptyMap());
  }

  private List<ApiNamingRule> loadApiNamingRules(@Nonnull ContextualKey<ServiceIdentifier> key) {
    try {
      return key
          .callInContext(
              () ->
                  spanProcessingConfigServiceBlockingStub.getAllApiNamingRules(
                      GetAllApiNamingRulesRequest.newBuilder().build()))
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
