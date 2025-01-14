package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter;

import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleData;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.time.Duration;
import java.util.Collections;
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

  // Cached on hash of user-attribution
  private final LoadingCache<ContextualKey<UserAttributionRuleData>, DerivationRule> jexlCache;
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

  public DerivationRule convert(
      RequestContext requestContext, UserAttributionRuleData userAttributionRuleData) {
    try {
      ContextualKey<UserAttributionRuleData> contextualKey =
          requestContext.buildInternalContextualKey(userAttributionRuleData);
      return jexlCache.get(contextualKey);
    } catch (Exception e) {
      log.error(
          "Error retrieving converted user attribution jexl from cache for tenant - {}: {}",
          requestContext.getTenantId(),
          userAttributionRuleData.getName(),
          e);
      return DerivationRule.getDefaultInstance();
    }
  }

  private DerivationRule loadValue(ContextualKey<UserAttributionRuleData> contextualKey) {
    return userAttributionJexlGenerator.convert(contextualKey.getData());
  }
}
