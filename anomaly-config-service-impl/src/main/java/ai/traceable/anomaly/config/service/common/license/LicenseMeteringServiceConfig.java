package ai.traceable.anomaly.config.service.common.license;

import com.typesafe.config.Config;
import java.time.Duration;

public class LicenseMeteringServiceConfig {

  private static final String HOST_CONFIG_NAME = "host";
  private static final String PORT_CONFIG_NAME = "port";
  private static final String CALL_TIMEOUT_CONFIG_NAME = "call.timeout.ms";
  private static final String CACHE_EXPIRY_DURATION_CONFIG_NAME = "cache.expiry.duration";
  private static final String CACHE_MAX_SIZE_CONFIG_NAME = "cache.max.size";

  private final String host;
  private final int port;
  private final long callTimeoutInMs;
  private final Duration cacheExpiryDuration;
  private final long cacheMaxSize;

  public LicenseMeteringServiceConfig(Config config) {
    host = config.getString(HOST_CONFIG_NAME);
    port = config.getInt(PORT_CONFIG_NAME);
    callTimeoutInMs = config.getLong(CALL_TIMEOUT_CONFIG_NAME);
    cacheExpiryDuration = config.getDuration(CACHE_EXPIRY_DURATION_CONFIG_NAME);
    cacheMaxSize = config.getInt(CACHE_MAX_SIZE_CONFIG_NAME);
  }

  public String getHost() {
    return this.host;
  }

  public int getPort() {
    return this.port;
  }

  public long getCallTimeoutInMs() {
    return callTimeoutInMs;
  }

  public Duration getCacheExpiryDuration() {
    return cacheExpiryDuration;
  }

  public long getCacheMaxSize() {
    return cacheMaxSize;
  }
}
