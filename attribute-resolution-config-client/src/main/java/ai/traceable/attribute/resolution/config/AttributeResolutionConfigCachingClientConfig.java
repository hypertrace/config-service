package ai.traceable.attribute.resolution.config;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AttributeResolutionConfigCachingClientConfig {
  private static final Joiner DOT_JOINER = Joiner.on(".");
  private static final String CACHE_CONFIG_PREFIX = "attribute.resolution.config.cache";
  private static final String CACHE_MAX_SIZE = DOT_JOINER.join(CACHE_CONFIG_PREFIX, "max.size");
  private static final String CACHE_MAX_THREAD_POOL_SIZE =
      DOT_JOINER.join(CACHE_CONFIG_PREFIX, "max.thread.pool.size");
  private static final String CACHE_REFRESH_DURATION =
      DOT_JOINER.join(CACHE_CONFIG_PREFIX, "refresh.duration");
  private static final String CACHE_EXPIRATION_DURATION =
      DOT_JOINER.join(CACHE_CONFIG_PREFIX, "expiration.duration");
  private static final String CLIENT_TIMEOUT_DURATION =
      DOT_JOINER.join(CACHE_CONFIG_PREFIX, "timeout.duration");
  private static final String CACHE_NAME = DOT_JOINER.join(CACHE_CONFIG_PREFIX, "name");

  private static final int DEFAULT_MAX_SIZE = 1000;
  private static final int DEFAULT_MAX_THREAD_POOL_SIZE = 1;
  private static final Duration DEFAULT_REFRESH_DURATION = Duration.ofMinutes(10);
  private static final Duration DEFAULT_EXPIRATION_DURATION = Duration.ofMinutes(20);
  private static final Duration DEFAULT_TIMEOUT_DURATION = Duration.ofSeconds(10);

  private int maxSize;
  private int maxThreadPoolSize;
  private Duration timeoutDuration;
  private Duration refreshDuration;
  private Duration expirationDuration;
  private String cacheName;
  private String cacheThreadFactoryName;

  public static AttributeResolutionConfigCachingClientConfig from(Config config) {
    String cacheName = config.getString(CACHE_NAME);
    return new AttributeResolutionConfigCachingClientConfig(
        config.hasPath(CACHE_MAX_SIZE) ? config.getInt(CACHE_MAX_SIZE) : DEFAULT_MAX_SIZE,
        config.hasPath(CACHE_MAX_THREAD_POOL_SIZE)
            ? config.getInt(CACHE_MAX_THREAD_POOL_SIZE)
            : DEFAULT_MAX_THREAD_POOL_SIZE,
        config.hasPath(CLIENT_TIMEOUT_DURATION)
            ? config.getDuration(CLIENT_TIMEOUT_DURATION)
            : DEFAULT_TIMEOUT_DURATION,
        config.hasPath(CACHE_REFRESH_DURATION)
            ? config.getDuration(CACHE_REFRESH_DURATION)
            : DEFAULT_REFRESH_DURATION,
        config.hasPath(CACHE_EXPIRATION_DURATION)
            ? config.getDuration(CACHE_EXPIRATION_DURATION)
            : DEFAULT_EXPIRATION_DURATION,
        cacheName,
        buildThreadFactoryName(cacheName));
  }

  private static String buildThreadFactoryName(String cacheName) {
    return cacheName + "-%d";
  }
}
