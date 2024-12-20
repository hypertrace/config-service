package ai.traceable.entity.fetcher.cache.config;

import ai.traceable.platform.cache.TimedCacheConfig;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import java.time.Duration;
import lombok.Getter;

@Getter
public class CachedEntityFetcherConfig {
  private static final String CACHED_ENTITY_FETCHER_CONFIG_PREFIX = "entity.fetcher.cache";
  private static final String SERVICE_MAPPING_CACHE_CONFIG = "service.mapping.cache";
  private static final String API_MAPPING_CACHE_CONFIG = "api.mapping.cache";
  private final TimedCacheConfig serviceMappingCacheConfig;
  private final Duration expireAfterAccessDuration;
  private final Duration refreshAfterWriteDuration;
  private final int maxSize;

  @Inject
  public CachedEntityFetcherConfig(Config config) {
    Config rootConfig = config.getConfig(CACHED_ENTITY_FETCHER_CONFIG_PREFIX);
    this.serviceMappingCacheConfig =
        new TimedCacheConfig(rootConfig.getConfig(SERVICE_MAPPING_CACHE_CONFIG));
    Config apiCacheConfig = rootConfig.getConfig(API_MAPPING_CACHE_CONFIG);
    expireAfterAccessDuration = apiCacheConfig.getDuration("expireAfterAccessDuration");
    refreshAfterWriteDuration = apiCacheConfig.getDuration("refreshAfterWriteDuration");
    maxSize = apiCacheConfig.getInt("maxSize");
  }
}
