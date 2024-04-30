package ai.traceable.localprocessing.config.service.apinaming.http;

import ai.traceable.localprocessing.config.service.apinaming.http.utils.ServiceIdentifier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
public class DelegateEntityCacheLoader
    extends CacheLoader<ContextualKey<ServiceIdentifier>, Optional<String>> {

  private static final String CACHE_REFRESH_DURATION =
      "api.naming.config.entity.fetcher.cache.refreshAfterWriteDuration";
  private static final String CACHE_EXPIRATION_DURATION =
      "api.naming.config.entity.fetcher.cache.expireAfterWriteDuration";
  private static final String MAXIMUM_CACHE_SIZE =
      "api.naming.config.entity.fetcher.cache.maximumSize";
  private static final String SERVICE_ENTITY_CACHE_NAME = "serviceEntityCache";
  private static final Duration CACHE_REFRESH_DURATION_DEFAULT = Duration.ofHours(12);
  private static final Duration CACHE_EXPIRATION_DURATION_DEFAULT = Duration.ofHours(24);
  private static final long MAXIMUM_CACHE_SIZE_DEFAULT = 1000;

  private final LoadingCache<ContextualKey<ServiceIdentifier>, Optional<String>> serviceEntityCache;

  @Inject
  public DelegateEntityCacheLoader(Config config, EntityCacheLoader entityCacheLoader) {
    Duration cacheRefreshDuration =
        config.hasPath(CACHE_REFRESH_DURATION)
            ? config.getDuration(CACHE_REFRESH_DURATION)
            : CACHE_REFRESH_DURATION_DEFAULT;
    Duration cacheExpiryDuration =
        config.hasPath(CACHE_EXPIRATION_DURATION)
            ? config.getDuration(CACHE_EXPIRATION_DURATION)
            : CACHE_EXPIRATION_DURATION_DEFAULT;
    long maximumCacheSize =
        config.hasPath(MAXIMUM_CACHE_SIZE)
            ? config.getLong(MAXIMUM_CACHE_SIZE)
            : MAXIMUM_CACHE_SIZE_DEFAULT;

    this.serviceEntityCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheRefreshDuration.toMillis(), TimeUnit.MILLISECONDS)
            .expireAfterWrite(cacheExpiryDuration.toMillis(), TimeUnit.MILLISECONDS)
            .maximumSize(maximumCacheSize)
            .recordStats()
            .build(entityCacheLoader);

    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        SERVICE_ENTITY_CACHE_NAME, serviceEntityCache, Collections.emptyMap(), maximumCacheSize);
  }

  @Override
  public Optional<String> load(
      @Nonnull ContextualKey<ServiceIdentifier> serviceIdentifierContextualKey) {
    try {
      Optional<String> maybeServiceId = serviceEntityCache.get(serviceIdentifierContextualKey);
      if (maybeServiceId.isEmpty()) {
        serviceEntityCache.invalidate(serviceIdentifierContextualKey);
        ServiceIdentifier serviceIdentifier = serviceIdentifierContextualKey.getData();
        log.debug(
            "Could not fetch entity for tenant id:{}, service name: {} and environment : {}",
            serviceIdentifierContextualKey.getContext().getTenantId(),
            serviceIdentifier.getServiceName(),
            serviceIdentifier.getEnvironment());
      }
      return maybeServiceId;
    } catch (Exception e) {
      ServiceIdentifier serviceIdentifier = serviceIdentifierContextualKey.getData();
      log.debug(
          "Could not fetch entity for tenant id:{}, service name: {} and environment : {}",
          serviceIdentifierContextualKey.getContext().getTenantId(),
          serviceIdentifier.getServiceName(),
          serviceIdentifier.getEnvironment(),
          e);
      return Optional.empty();
    }
  }

  @Override
  public Map<ContextualKey<ServiceIdentifier>, Optional<String>> loadAll(
      @Nonnull Iterable<? extends ContextualKey<ServiceIdentifier>> keys) {
    try {
      Map<ContextualKey<ServiceIdentifier>, Optional<String>> maybeServiceIds =
          serviceEntityCache.getAll(keys);
      for (ContextualKey<ServiceIdentifier> serviceIdentifierContextualKey :
          maybeServiceIds.keySet()) {
        Optional<String> maybeServiceId = maybeServiceIds.get(serviceIdentifierContextualKey);
        if (maybeServiceId == null || maybeServiceId.isEmpty()) {
          serviceEntityCache.invalidate(serviceIdentifierContextualKey);
          log.debug(
              "Could not fetch entity for tenant id:{}, and contextual service identifier: {}",
              keys.iterator().next().getContext().getTenantId(),
              serviceIdentifierContextualKey);
        }
      }
      return maybeServiceIds;
    } catch (Exception e) {
      log.debug(
          "Could not fetch entities for tenant id:{}, and contextual service identifiers: {}",
          keys.iterator().next().getContext().getTenantId(),
          keys,
          e);
      return Map.of();
    }
  }
}
