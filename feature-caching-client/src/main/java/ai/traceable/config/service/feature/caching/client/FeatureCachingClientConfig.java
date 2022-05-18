package ai.traceable.config.service.feature.caching.client;

import com.typesafe.config.Config;
import java.time.Duration;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class FeatureCachingClientConfig {
  private static final String FEATURE_FLAG_SERVICE_CONFIG = "feature.flag.service.config";
  private static final String FEATURE_FLAG_SERVICE_CONFIG_HOST =
      FEATURE_FLAG_SERVICE_CONFIG + ".host";
  private static final String FEATURE_FLAG_SERVICE_CONFIG_PORT =
      FEATURE_FLAG_SERVICE_CONFIG + ".port";
  private static final String FEATURE_FLAG_SERVICE_CONFIG_REQUEST_TIMEOUT =
      FEATURE_FLAG_SERVICE_CONFIG + ".request.timeout";
  private static final String FEATURE_FLAG_SERVICE_CACHE_CONFIG =
      FEATURE_FLAG_SERVICE_CONFIG + ".cache";
  private static final String FEATURE_FLAG_SERVICE_CACHE_CONFIG_REFRESH_DURATION =
      FEATURE_FLAG_SERVICE_CACHE_CONFIG + ".refresh.duration";
  private static final String FEATURE_FLAG_SERVICE_CACHE_CONFIG_EXPIRATION_DURATION =
      FEATURE_FLAG_SERVICE_CACHE_CONFIG + ".expiration.duration";
  private static final String FEATURE_FLAG_SERVICE_CACHE_CONFIG_THREAD_POOL_SIZE =
      FEATURE_FLAG_SERVICE_CACHE_CONFIG + ".thread.pool.size";
  String host;
  int port;
  Duration requestTimeout;
  Duration expirationDuration;
  Duration refreshDuration;
  int threadPoolSize;

  public static FeatureCachingClientConfig fromConfig(Config config) {
    return FeatureCachingClientConfig.builder()
        .host(config.getString(FEATURE_FLAG_SERVICE_CONFIG_HOST))
        .port(config.getInt(FEATURE_FLAG_SERVICE_CONFIG_PORT))
        .requestTimeout(config.getDuration(FEATURE_FLAG_SERVICE_CONFIG_REQUEST_TIMEOUT))
        .refreshDuration(config.getDuration(FEATURE_FLAG_SERVICE_CACHE_CONFIG_REFRESH_DURATION))
        .expirationDuration(
            config.getDuration(FEATURE_FLAG_SERVICE_CACHE_CONFIG_EXPIRATION_DURATION))
        .threadPoolSize(config.getInt(FEATURE_FLAG_SERVICE_CACHE_CONFIG_THREAD_POOL_SIZE))
        .build();
  }
}
