package ai.traceable.anomaly.config.service.modsec;

import com.typesafe.config.Config;
import jakarta.inject.Singleton;
import java.time.Duration;

@Singleton
public class ModsecConfigServiceConfig {
  private static final String WEB_APP_CONFIG_CONTEXT_CACHE_CONFIG_KEY = "webAppConfigContextCache";
  private static final String WEB_APP_CONFIG_CONTEXT_CACHE_MAX_SIZE =
      WEB_APP_CONFIG_CONTEXT_CACHE_CONFIG_KEY + ".maxSize";
  private static final String WEB_APP_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION =
      WEB_APP_CONFIG_CONTEXT_CACHE_CONFIG_KEY + ".refreshAfterWriteDuration";
  private static final String WEB_APP_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE =
      WEB_APP_CONFIG_CONTEXT_CACHE_CONFIG_KEY + ".threadPoolSize";
  private static final int DEFAULT_WEB_APP_CONFIG_CONTEXT_CACHE_MAX_SIZE = 400;
  private static final Duration DEFAULT_WEB_APP_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION =
      Duration.ofMinutes(5);
  private static final int DEFAULT_WEB_APP_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE = 4;
  private final Config config;

  public ModsecConfigServiceConfig(Config config) {
    this.config = config;
  }

  public int getWebAppConfigContextCacheMaxSize() {
    return config.hasPath(WEB_APP_CONFIG_CONTEXT_CACHE_MAX_SIZE)
        ? config.getInt(WEB_APP_CONFIG_CONTEXT_CACHE_MAX_SIZE)
        : DEFAULT_WEB_APP_CONFIG_CONTEXT_CACHE_MAX_SIZE;
  }

  public Duration getWebAppConfigContextCacheRefreshAfterWriteDuration() {
    return config.hasPath(WEB_APP_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION)
        ? config.getDuration(WEB_APP_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION)
        : DEFAULT_WEB_APP_CONFIG_CONTEXT_CACHE_REFRESH_AFTER_WRITE_DURATION;
  }

  public int getWebAppConfigContextCacheThreadPoolSize() {
    return config.hasPath(WEB_APP_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE)
        ? config.getInt(WEB_APP_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE)
        : DEFAULT_WEB_APP_CONFIG_CONTEXT_CACHE_THREAD_POOL_SIZE;
  }
}
