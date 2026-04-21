package ai.traceable.aiapp.protection.config.service.firewall;

import ai.traceable.data.classification.cache.config.DataClassificationInfoCachingClientConfig;
import com.typesafe.config.Config;
import jakarta.inject.Singleton;
import java.time.Duration;
import lombok.Getter;

@Getter
@Singleton
public class AiAppConfigServiceConfig {
  private static final String AI_APP_CONFIG_CONTEXT_CACHE_CONFIG_KEY = "aiAppConfigContextCache";
  private static final String AI_APP_CONFIG_CONTEXT_CACHE_MAX_SIZE =
      AI_APP_CONFIG_CONTEXT_CACHE_CONFIG_KEY + ".maxSize";
  private static final String AI_APP_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION =
      AI_APP_CONFIG_CONTEXT_CACHE_CONFIG_KEY + ".refreshAfterWriteDuration";
  private static final String AI_APP_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE =
      AI_APP_CONFIG_CONTEXT_CACHE_CONFIG_KEY + ".threadPoolSize";
  private static final int DEFAULT_AI_APP_CONFIG_CONTEXT_CACHE_MAX_SIZE = 400;
  private static final Duration DEFAULT_AI_APP_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION =
      Duration.ofMinutes(5);
  private static final int DEFAULT_AI_APP_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE = 4;
  private final int aiAppConfigContextCacheMaxSize;
  private final Duration aiAppConfigContextCacheRefreshAfterWriteDuration;
  private final int aiAppConfigContextCacheThreadPoolSize;
  private final DataClassificationInfoCachingClientConfig dataClassificationInfoCachingClientConfig;

  public AiAppConfigServiceConfig(Config config) {
    this.aiAppConfigContextCacheMaxSize =
        config.hasPath(AI_APP_CONFIG_CONTEXT_CACHE_MAX_SIZE)
            ? config.getInt(AI_APP_CONFIG_CONTEXT_CACHE_MAX_SIZE)
            : DEFAULT_AI_APP_CONFIG_CONTEXT_CACHE_MAX_SIZE;
    this.aiAppConfigContextCacheRefreshAfterWriteDuration =
        config.hasPath(AI_APP_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION)
            ? config.getDuration(AI_APP_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION)
            : DEFAULT_AI_APP_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION;
    this.aiAppConfigContextCacheThreadPoolSize =
        config.hasPath(AI_APP_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE)
            ? config.getInt(AI_APP_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE)
            : DEFAULT_AI_APP_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE;
    this.dataClassificationInfoCachingClientConfig =
        DataClassificationInfoCachingClientConfig.from(config);
  }
}
