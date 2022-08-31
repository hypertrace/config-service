package ai.traceable.blocking.config.service;

import com.typesafe.config.Config;
import java.time.Duration;

public class BlockingDataCacheConfig {
  private static final String CACHE_CONFIG_NAME = "cache";
  public static final String MAX_CACHE_SIZE = "maxCacheSize";
  public static final String EXPIRE_AFTER_WRITE_DURATION = "expireAfterWriteDuration";
  public static final String REFRESH_AFTER_WRITE_DURATION = "refreshAfterWriteDuration";

  private final long maxCacheSize;
  private final Duration writeExpirationDuration;
  private final Duration refreshExpirationDuration;

  public BlockingDataCacheConfig(Config config) {
    Config cacheConfig = config.getConfig(CACHE_CONFIG_NAME);
    maxCacheSize = cacheConfig.getLong(MAX_CACHE_SIZE);
    writeExpirationDuration = cacheConfig.getDuration(EXPIRE_AFTER_WRITE_DURATION);
    refreshExpirationDuration = cacheConfig.getDuration(REFRESH_AFTER_WRITE_DURATION);
  }

  public long getMaxCacheSize() {
    return maxCacheSize;
  }

  public Duration getWriteExpirationDuration() {
    return writeExpirationDuration;
  }

  public Duration getRefreshExpirationDuration() {
    return refreshExpirationDuration;
  }
}
