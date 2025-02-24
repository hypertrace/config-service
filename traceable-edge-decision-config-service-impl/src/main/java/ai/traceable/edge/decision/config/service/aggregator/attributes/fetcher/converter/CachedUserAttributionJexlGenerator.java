package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter;

import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleData;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Singleton
@Slf4j
public class CachedUserAttributionJexlGenerator {
  private static final String USER_ATTRIBUTION_JEXL_GENERATOR_CACHE =
      "UserAttributionJexlGeneratorCache";
  private static final int MAX_CACHE_SIZE = 10000;
  private static final RateLimiter LOG_RATE_LIMITER = RateLimiter.create(0.01);

  // Cached on hash of user-attribution
  private final LoadingCache<ContextualKey<UserAttributionRuleData>, Optional<DerivationRule>>
      jexlCache;
  private final UserAttributionJexlGenerator userAttributionJexlGenerator;

  @Inject
  public CachedUserAttributionJexlGenerator(
      UserAttributionJexlGenerator userAttributionJexlGenerator) {
    jexlCache =
        CacheBuilder.newBuilder()
            .maximumSize(MAX_CACHE_SIZE)
            .expireAfterAccess(Duration.ofMinutes(5))
            .expireAfterWrite(Duration.ofHours(2))
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(this::loadValue), Executors.newSingleThreadExecutor()));

    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        USER_ATTRIBUTION_JEXL_GENERATOR_CACHE, jexlCache, Collections.emptyMap(), MAX_CACHE_SIZE);

    this.userAttributionJexlGenerator = userAttributionJexlGenerator;
  }

  public Optional<DerivationRule> convert(
      RequestContext requestContext, UserAttributionRuleData userAttributionRuleData) {
    ContextualKey<UserAttributionRuleData> contextualKey =
        requestContext.buildInternalContextualKey(userAttributionRuleData);
    return jexlCache.getUnchecked(contextualKey);
  }

  private Optional<DerivationRule> loadValue(ContextualKey<UserAttributionRuleData> contextualKey) {
    UserAttributionRuleData ruleData = contextualKey.getData();
    try {
      if (!ruleData.hasUserIdRule()) {
        return Optional.empty();
      }
      return Optional.of(userAttributionJexlGenerator.convert(contextualKey.getData()));
    } catch (Exception e) {
      if (LOG_RATE_LIMITER.tryAcquire()) {
        log.warn(
            "Error converting user attribution to jexl for tenant {} : {}",
            contextualKey.getContext().getTenantId(),
            ruleData.getName(),
            e);
      }
      return Optional.empty();
    }
  }
}
