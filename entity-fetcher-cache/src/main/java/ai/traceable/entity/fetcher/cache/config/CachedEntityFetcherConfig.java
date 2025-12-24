package ai.traceable.entity.fetcher.cache.config;

import ai.traceable.platform.cache.TimedCacheConfig;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import lombok.Getter;

@Getter
public class CachedEntityFetcherConfig {

  private static final String CACHED_ENTITY_FETCHER_CONFIG_PREFIX = "entity.fetcher.cache";
  private static final String SERVICE_MAPPING_CACHE_CONFIG = "service.mapping.cache";
  private static final String API_MAPPING_CACHE_CONFIG = "api.mapping.cache";
  private static final String SECURITY_SCHEME_CACHE_CONFIG = "security.scheme.cache";
  public static final String EXPIRE_AFTER_ACCESS_DURATION = "expireAfterAccessDuration";
  public static final String REFRESH_AFTER_WRITE_DURATION = "refreshAfterWriteDuration";
  public static final String MAX_SIZE = "maxSize";
  private static final int DEFAULT_SECURITY_SCHEME_CACHE_MAX_SIZE = 10000;
  private static final Duration DEFAULT_SECURITY_SCHEME_CACHE_EXPIRE_AFTER_ACCESS_DURATION =
      Duration.ofHours(1);
  private static final Duration DEFAULT_SECURITY_SCHEME_CACHE_REFRESH_AFTER_WRITE_DURATION =
      Duration.ofMinutes(10);
  private final TimedCacheConfig serviceMappingCacheConfig;
  private final Duration expireAfterAccessDuration;
  private final Duration refreshAfterWriteDuration;
  private final int maxSize;
  private final Duration securitySchemeCacheExpireAfterAccessDuration;
  private final Duration securitySchemeCacheRefreshAfterWriteDuration;
  private final int securitySchemeCacheMaxSize;

  @Inject
  public CachedEntityFetcherConfig(Config config) {
    Config rootConfig = config.getConfig(CACHED_ENTITY_FETCHER_CONFIG_PREFIX);
    this.serviceMappingCacheConfig =
        new TimedCacheConfig(rootConfig.getConfig(SERVICE_MAPPING_CACHE_CONFIG));
    Config apiCacheConfig = rootConfig.getConfig(API_MAPPING_CACHE_CONFIG);
    expireAfterAccessDuration = apiCacheConfig.getDuration(EXPIRE_AFTER_ACCESS_DURATION);
    refreshAfterWriteDuration = apiCacheConfig.getDuration(REFRESH_AFTER_WRITE_DURATION);
    maxSize = apiCacheConfig.getInt(MAX_SIZE);
    Config securitySchemeCacheConfig =
        rootConfig.hasPath(SECURITY_SCHEME_CACHE_CONFIG)
            ? rootConfig.getConfig(SECURITY_SCHEME_CACHE_CONFIG)
            : ConfigFactory.empty();
    securitySchemeCacheExpireAfterAccessDuration =
        securitySchemeCacheConfig.hasPath(EXPIRE_AFTER_ACCESS_DURATION)
            ? securitySchemeCacheConfig.getDuration(EXPIRE_AFTER_ACCESS_DURATION)
            : DEFAULT_SECURITY_SCHEME_CACHE_EXPIRE_AFTER_ACCESS_DURATION;
    securitySchemeCacheRefreshAfterWriteDuration =
        securitySchemeCacheConfig.hasPath(REFRESH_AFTER_WRITE_DURATION)
            ? securitySchemeCacheConfig.getDuration(REFRESH_AFTER_WRITE_DURATION)
            : DEFAULT_SECURITY_SCHEME_CACHE_REFRESH_AFTER_WRITE_DURATION;
    securitySchemeCacheMaxSize =
        securitySchemeCacheConfig.hasPath(MAX_SIZE)
            ? securitySchemeCacheConfig.getInt(MAX_SIZE)
            : DEFAULT_SECURITY_SCHEME_CACHE_MAX_SIZE;
  }
}
