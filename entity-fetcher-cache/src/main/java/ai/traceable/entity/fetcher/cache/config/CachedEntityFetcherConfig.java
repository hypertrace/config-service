package ai.traceable.entity.fetcher.cache.config;

import ai.traceable.platform.cache.TimedCacheConfig;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import lombok.Getter;

@Getter
public class CachedEntityFetcherConfig {
  private static final String CACHED_ENTITY_FETCHER_CONFIG_PREFIX = "entity.fetcher.cache.";
  private static final String SERVICE_MAPPING_CACHE_CONFIG =
      CACHED_ENTITY_FETCHER_CONFIG_PREFIX + "service.mapping.cache";
  private final TimedCacheConfig serviceMappingCacheConfig;

  @Inject
  public CachedEntityFetcherConfig(Config config) {
    this.serviceMappingCacheConfig =
        new TimedCacheConfig(config.getConfig(SERVICE_MAPPING_CACHE_CONFIG));
  }
}
