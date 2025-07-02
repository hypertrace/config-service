package ai.traceable.genai.system.discovery.config;

import com.typesafe.config.Config;
import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class GenAiSystemDiscoveryConfigCachingClientConfig {
  private static final String GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_MAX_SIZE =
      "genai.system.discovery.config.cache.max.size";
  private static final String GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_MAX_THREAD_POOL_SIZE =
      "genai.system.discovery.config.cache.max.thread.pool.size";
  private static final String GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_REFRESH_DURATION =
      "genai.system.discovery.config.cache.refresh.duration";
  private static final String GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_EXPIRATION_DURATION =
      "genai.system.discovery.config.cache.expiration.duration";
  private static final String GENAI_SYSTEM_DISCOVERY_CONFIG_CLIENT_TIMEOUT_DURATION =
      "genai.system.discovery.config.cache.timeout.duration";
  private static final String GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_NAME =
      "genai.system.discovery.config.cache.name";

  private static final int DEFAULT_GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_MAX_SIZE = 1000;
  private static final int DEFAULT_GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_MAX_THREAD_POOL_SIZE = 1;
  private static final Duration DEFAULT_GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_REFRESH_DURATION =
      Duration.ofMinutes(10);
  private static final Duration DEFAULT_GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_EXPIRATION_DURATION =
      Duration.ofMinutes(20);
  private static final Duration DEFAULT_GENAI_SYSTEM_DISCOVERY_CONFIG_CLIENT_TIMEOUT_DURATION =
      Duration.ofSeconds(10);

  private int maxSize;
  private int maxThreadPoolSize;
  private Duration timeoutDuration;
  private Duration refreshDuration;
  private Duration expirationDuration;
  private String genAiSystemDiscoveryConfigCacheName;
  private String genAiSystemDiscoveryConfigCacheThreadFactoryName;

  public static GenAiSystemDiscoveryConfigCachingClientConfig from(Config config) {
    String genAiSystemDiscoveryConfigCacheName =
        config.getString(GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_NAME);
    return new GenAiSystemDiscoveryConfigCachingClientConfig(
        config.hasPath(GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_MAX_SIZE)
            ? config.getInt(GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_MAX_SIZE)
            : DEFAULT_GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_MAX_SIZE,
        config.hasPath(GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_MAX_THREAD_POOL_SIZE)
            ? config.getInt(GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_MAX_THREAD_POOL_SIZE)
            : DEFAULT_GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_MAX_THREAD_POOL_SIZE,
        config.hasPath(GENAI_SYSTEM_DISCOVERY_CONFIG_CLIENT_TIMEOUT_DURATION)
            ? config.getDuration(GENAI_SYSTEM_DISCOVERY_CONFIG_CLIENT_TIMEOUT_DURATION)
            : DEFAULT_GENAI_SYSTEM_DISCOVERY_CONFIG_CLIENT_TIMEOUT_DURATION,
        config.hasPath(GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_REFRESH_DURATION)
            ? config.getDuration(GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_REFRESH_DURATION)
            : DEFAULT_GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_REFRESH_DURATION,
        config.hasPath(GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_EXPIRATION_DURATION)
            ? config.getDuration(GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_EXPIRATION_DURATION)
            : DEFAULT_GENAI_SYSTEM_DISCOVERY_CONFIG_CACHE_EXPIRATION_DURATION,
        genAiSystemDiscoveryConfigCacheName,
        getThreadFactoryName(genAiSystemDiscoveryConfigCacheName));
  }

  private static String getThreadFactoryName(String genAiSystemDiscoveryConfigCacheName) {
    return genAiSystemDiscoveryConfigCacheName + "-%d";
  }
}
