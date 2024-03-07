package ai.traceable.data.classification.cache.config;

import com.typesafe.config.Config;
import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class DataClassificationInfoCachingClientConfig {
  private static final String DATA_CLASSIFICATION_INFO_CACHE_MAX_SIZE =
      "data.classification.info.cache.max.size";
  private static final String DATA_CLASSIFICATION_INFO_CACHE_MAX_THREAD_POOL_SIZE =
      "data.classification.info.cache.max.thread.pool.size";
  private static final String DATA_CLASSIFICATION_INFO_CACHE_REFRESH_DURATION =
      "data.classification.info.cache.refresh.duration";
  private static final String DATA_CLASSIFICATION_INFO_CACHE_EXPIRATION_DURATION =
      "data.classification.info.cache.expiration.duration";
  private static final String DATA_CLASSIFICATION_CLIENT_TIMEOUT_DURATION =
      "data.classification.info.cache.timeout.duration";
  private static final String CONSUMER_NAME_PATH = "data.classification.info.cache.consumer.name";
  private static final String SCHEMA_REGISTRY_URL_PATH =
      "data.classification.info.cache.schema.registry.url";

  private static final int DEFAULT_DATA_CLASSIFICATION_INFO_CACHE_MAX_SIZE = 1000;
  private static final int DEFAULT_DATA_CLASSIFICATION_INFO_CACHE_MAX_THREAD_POOL_SIZE = 1;
  private static final Duration DEFAULT_DATA_CLASSIFICATION_INFO_CACHE_REFRESH_DURATION =
      Duration.ofMinutes(10);
  private static final Duration DEFAULT_DATA_CLASSIFICATION_INFO_CACHE_EXPIRATION_DURATION =
      Duration.ofMinutes(20);
  private static final Duration DEFAULT_DATA_CLASSIFICATION_CLIENT_TIMEOUT_DURATION =
      Duration.ofSeconds(10);
  private static final String DATA_CLASSIFICATION_INFO_CACHE_NAME =
      "TraceEnricherDataClassificationInfoCache";
  private static final String DATA_CLASSIFICATION_INFO_CACHE_THREAD_FACTORY_NAME =
      "data-classification-info-cache-%d";
  private int maxSize;
  private int maxThreadPoolSize;
  private Duration timeoutDuration;
  private Duration refreshDuration;
  private Duration expirationDuration;
  private String dataClassificationInfoCacheName;
  private String dataClassificationInfoCacheThreadFactoryName;
  private String consumerName;
  private String schemaRegistryUrl;

  public static DataClassificationInfoCachingClientConfig from(Config config) {
    return new DataClassificationInfoCachingClientConfig(
        config.hasPath(DATA_CLASSIFICATION_INFO_CACHE_MAX_SIZE)
            ? config.getInt(DATA_CLASSIFICATION_INFO_CACHE_MAX_SIZE)
            : DEFAULT_DATA_CLASSIFICATION_INFO_CACHE_MAX_SIZE,
        config.hasPath(DATA_CLASSIFICATION_INFO_CACHE_MAX_THREAD_POOL_SIZE)
            ? config.getInt(DATA_CLASSIFICATION_INFO_CACHE_MAX_THREAD_POOL_SIZE)
            : DEFAULT_DATA_CLASSIFICATION_INFO_CACHE_MAX_THREAD_POOL_SIZE,
        config.hasPath(DATA_CLASSIFICATION_INFO_CACHE_REFRESH_DURATION)
            ? config.getDuration(DATA_CLASSIFICATION_INFO_CACHE_REFRESH_DURATION)
            : DEFAULT_DATA_CLASSIFICATION_INFO_CACHE_REFRESH_DURATION,
        config.hasPath(DATA_CLASSIFICATION_INFO_CACHE_EXPIRATION_DURATION)
            ? config.getDuration(DATA_CLASSIFICATION_INFO_CACHE_EXPIRATION_DURATION)
            : DEFAULT_DATA_CLASSIFICATION_INFO_CACHE_EXPIRATION_DURATION,
        config.hasPath(DATA_CLASSIFICATION_CLIENT_TIMEOUT_DURATION)
            ? config.getDuration(DATA_CLASSIFICATION_CLIENT_TIMEOUT_DURATION)
            : DEFAULT_DATA_CLASSIFICATION_CLIENT_TIMEOUT_DURATION,
        DATA_CLASSIFICATION_INFO_CACHE_THREAD_FACTORY_NAME,
        DATA_CLASSIFICATION_INFO_CACHE_NAME,
        config.getString(CONSUMER_NAME_PATH),
        config.getString(SCHEMA_REGISTRY_URL_PATH));
  }
}
