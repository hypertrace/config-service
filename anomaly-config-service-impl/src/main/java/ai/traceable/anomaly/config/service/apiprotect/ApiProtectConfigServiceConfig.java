package ai.traceable.anomaly.config.service.apiprotect;

import com.typesafe.config.Config;
import jakarta.inject.Singleton;
import java.time.Duration;
import lombok.Getter;

@Getter
@Singleton
public class ApiProtectConfigServiceConfig {
  private static final String API_PROTECT_CONFIG_CONTEXT_CACHE_CONFIG_KEY =
      "apiProtectConfigContextCache";
  private static final String API_PROTECT_CONFIG_CONTEXT_CACHE_MAX_SIZE =
      API_PROTECT_CONFIG_CONTEXT_CACHE_CONFIG_KEY + ".maxSize";
  private static final String API_PROTECT_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION =
      API_PROTECT_CONFIG_CONTEXT_CACHE_CONFIG_KEY + ".refreshAfterWriteDuration";
  private static final String API_PROTECT_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE =
      API_PROTECT_CONFIG_CONTEXT_CACHE_CONFIG_KEY + ".threadPoolSize";
  private static final int DEFAULT_API_PROTECT_CONFIG_CONTEXT_CACHE_MAX_SIZE = 400;
  private static final Duration
      DEFAULT_API_PROTECT_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION = Duration.ofMinutes(5);
  private static final int DEFAULT_API_PROTECT_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE = 4;
  private final int apiProtectConfigContextCacheMaxSize;
  private final Duration apiProtectConfigContextCacheRefreshAfterWriteDuration;
  private final int apiProtectConfigContextCacheThreadPoolSize;

  public ApiProtectConfigServiceConfig(Config config) {
    this.apiProtectConfigContextCacheMaxSize =
        config.hasPath(API_PROTECT_CONFIG_CONTEXT_CACHE_MAX_SIZE)
            ? config.getInt(API_PROTECT_CONFIG_CONTEXT_CACHE_MAX_SIZE)
            : DEFAULT_API_PROTECT_CONFIG_CONTEXT_CACHE_MAX_SIZE;
    this.apiProtectConfigContextCacheRefreshAfterWriteDuration =
        config.hasPath(API_PROTECT_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION)
            ? config.getDuration(API_PROTECT_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION)
            : DEFAULT_API_PROTECT_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION;
    this.apiProtectConfigContextCacheThreadPoolSize =
        config.hasPath(API_PROTECT_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE)
            ? config.getInt(API_PROTECT_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE)
            : DEFAULT_API_PROTECT_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE;
  }
}
