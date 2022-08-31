package ai.traceable.blocking.config.service.entity;

import ai.traceable.blocking.config.service.BlockingDataCacheConfig;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
public class EntityFetcher {

  private static final String ENVIRONMENT_ENTITY_CACHE_NAME = "environmentEntityCache";
  private final LoadingCache<ContextualKey<Void>, Map<String, String>> environmentEntityCache;

  @Inject
  public EntityFetcher(
      EntityQueryServiceConfig config, EnvironmentIdCacheLoader environmentIdCacheLoader) {
    BlockingDataCacheConfig cacheConfig = config.getCacheConfig();
    this.environmentEntityCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheConfig.getRefreshExpirationDuration())
            .expireAfterWrite(cacheConfig.getWriteExpirationDuration())
            .maximumSize(cacheConfig.getMaxCacheSize())
            .recordStats()
            .build(environmentIdCacheLoader);

    PlatformMetricsRegistry.registerCache(
        ENVIRONMENT_ENTITY_CACHE_NAME, environmentEntityCache, Collections.emptyMap());
  }

  public Optional<String> getEnvironmentId(RequestContext requestContext, String environment)
      throws ExecutionException {
    if (environment == null || environment.isEmpty()) {
      return Optional.empty();
    }
    return Optional.ofNullable(
        environmentEntityCache.get(requestContext.buildInternalContextualKey()).get(environment));
  }
}
