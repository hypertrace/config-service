package ai.traceable.localprocessing.config.service.apinaming.http;

import ai.traceable.localprocessing.config.service.apinaming.http.utils.ServiceIdentifier;
import ai.traceable.localprocessing.config.service.v1.ServiceRequest;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
public class EntityFetcher {
  private static final String CACHE_REFRESH_DURATION =
      "api.naming.config.entity.fetcher.cache.refreshAfterWriteDuration";
  private static final String CACHE_EXPIRATION_DURATION =
      "api.naming.config.entity.fetcher.cache.expireAfterWriteDuration";
  private static final String MAXIMUM_CACHE_SIZE =
      "api.naming.config.entity.fetcher.cache.maximumCacheSize";
  private static final String CACHE_NAME = "serviceEntityCache";
  private static final Duration CACHE_REFRESH_DURATION_DEFAULT = Duration.ofHours(12);
  private static final Duration CACHE_EXPIRATION_DURATION_DEFAULT = Duration.ofHours(24);
  private static final long MAXIMUM_CACHE_SIZE_DEFAULT = 1000;

  private final LoadingCache<ContextualKey<ServiceIdentifier>, Optional<String>> serviceEntityCache;

  @Inject
  public EntityFetcher(Config config, EntityCacheLoader entityCacheLoader) {
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
    PlatformMetricsRegistry.registerCache(CACHE_NAME, serviceEntityCache, Collections.emptyMap());
  }

  public Map<ServiceRequest, Optional<String>> getServiceIds(
      RequestContext requestContext,
      List<ServiceRequest> serviceRequests,
      Optional<String> environment)
      throws ExecutionException {
    List<ContextualKey<ServiceIdentifier>> serviceIdentifierContextualKeys = new ArrayList<>();
    Map<ContextualKey<ServiceIdentifier>, ServiceRequest> contextualKeyServiceRequestMap =
        new HashMap<>();
    Map<ServiceRequest, Optional<String>> serviceIdMap = new HashMap<>();
    for (ServiceRequest serviceRequest : serviceRequests) {
      ContextualKey<ServiceIdentifier> serviceIdentifierContextualKey =
          buildServiceIdentifierContextualKey(requestContext, serviceRequest, environment);
      Optional<String> serviceIdMaybe =
          serviceEntityCache.getIfPresent(serviceIdentifierContextualKey);
      if (serviceIdMaybe == null || serviceIdMaybe.isEmpty()) {
        serviceIdentifierContextualKeys.add(serviceIdentifierContextualKey);
        contextualKeyServiceRequestMap.put(serviceIdentifierContextualKey, serviceRequest);
        if (environment.isPresent()) {
          ContextualKey<ServiceIdentifier> serviceIdentifierContextualKeyWithoutEnvironment =
              buildServiceIdentifierContextualKey(requestContext, serviceRequest, Optional.empty());
          serviceIdentifierContextualKeys.add(serviceIdentifierContextualKeyWithoutEnvironment);
        }
      } else {
        serviceIdMap.put(serviceRequest, serviceIdMaybe);
      }
    }
    ImmutableMap<ContextualKey<ServiceIdentifier>, Optional<String>> loadedServiceIdMap =
        serviceEntityCache.getAll(serviceIdentifierContextualKeys);
    for (ServiceRequest serviceRequest : serviceRequests) {
      // some entities may have been created without environment as identifying attribute and later
      // when environment was available, they were not updated. So, we have the following backup
      // plan to get the serviceID. If for a request with environment, we do not obtain any entity,
      // we look for the same without environment as backup
      ContextualKey<ServiceIdentifier> serviceIdentifierContextualKey =
          buildServiceIdentifierContextualKey(requestContext, serviceRequest, environment);
      ContextualKey<ServiceIdentifier> serviceIdentifierContextualKeyWithoutEnvironment =
          buildServiceIdentifierContextualKey(requestContext, serviceRequest, Optional.empty());
      Optional<String> serviceIdMaybe = loadedServiceIdMap.get(serviceIdentifierContextualKey);
      if (serviceIdMaybe == null || serviceIdMaybe.isEmpty()) {
        Optional<String> serviceIdMapWithoutEnvironmentMaybe =
            loadedServiceIdMap.get(serviceIdentifierContextualKeyWithoutEnvironment);
        if (serviceIdMapWithoutEnvironmentMaybe != null
            && serviceIdMapWithoutEnvironmentMaybe.isPresent()) {
          serviceIdMap.put(serviceRequest, serviceIdMapWithoutEnvironmentMaybe);
        }
      } else {
        serviceIdMap.put(serviceRequest, serviceIdMaybe);
      }
    }
    return serviceIdMap;
  }

  private ContextualKey<ServiceIdentifier> buildServiceIdentifierContextualKey(
      RequestContext requestContext, ServiceRequest serviceRequest, Optional<String> environment) {
    return requestContext.buildInternalContextualKey(
        new ServiceIdentifier(serviceRequest.getServiceName(), environment));
  }
}
