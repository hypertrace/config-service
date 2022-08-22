package ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor.config;

import com.typesafe.config.Config;
import java.time.Duration;

public class ActorServiceCacheConfig {
  public static final String MAX_CACHE_SIZE = "maxCacheSize";
  public static final String EXPIRE_AFTER_WRITE_DURATION = "expireAfterWriteDuration";
  public static final String REFRESH_AFTER_WRITE_DURATION = "refreshAfterWriteDuration";

  private final long maxCacheSize;
  private final Duration writeExpirationDuration;
  private final Duration refreshExpirationDuration;

  public ActorServiceCacheConfig(Config config) {
    maxCacheSize = config.getLong(MAX_CACHE_SIZE);
    writeExpirationDuration = config.getDuration(EXPIRE_AFTER_WRITE_DURATION);
    refreshExpirationDuration = config.getDuration(REFRESH_AFTER_WRITE_DURATION);
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
