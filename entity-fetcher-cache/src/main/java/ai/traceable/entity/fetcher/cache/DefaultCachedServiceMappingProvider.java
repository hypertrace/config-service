package ai.traceable.entity.fetcher.cache;

import ai.traceable.entity.fetcher.cache.config.CachedEntityFetcherConfig;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Singleton
@Slf4j
class DefaultCachedServiceMappingProvider implements CachedServiceMappingProvider {
  private final EntityQueryServiceClient entityQueryServiceClient;
  private final LoadingCache<ContextualKey<String>, Optional<ServiceIdentifierEntity>>
      serviceEntitiesCache;

  @Inject
  DefaultCachedServiceMappingProvider(
      @Named(CachedServiceMappingProviderModule.SERVICE_MAPPING_CACHE_NAME) String cacheName,
      EntityQueryServiceClient entityQueryServiceClient,
      Clock clock,
      CachedEntityFetcherConfig cachedEntityFetcherConfig) {
    this.entityQueryServiceClient = entityQueryServiceClient;
    this.serviceEntitiesCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(cachedEntityFetcherConfig.getExpireAfterAccessDuration())
            .maximumSize(cachedEntityFetcherConfig.getMaxSize())
            .recordStats()
            .build(buildServiceEntitiesCacheLoader());
  }

  @Override
  public Optional<ServiceIdentifierEntity> getServiceIdentifierEntity(
      RequestContext requestContext, @NonNull String serviceId) {
    return serviceEntitiesCache.getUnchecked(requestContext.buildInternalContextualKey(serviceId));
  }

  @SneakyThrows
  @Override
  public Map<String, Optional<ServiceIdentifierEntity>> getServiceIdentifierEntities(
      RequestContext requestContext, @NonNull Set<String> serviceIds) {
    List<ContextualKey<String>> contextualKeys =
        serviceIds.stream()
            .map(requestContext::buildInternalContextualKey)
            .collect(Collectors.toUnmodifiableList());
    ImmutableMap<ContextualKey<String>, Optional<ServiceIdentifierEntity>> cachedValues =
        serviceEntitiesCache.getAll(contextualKeys);
    return cachedValues.entrySet().stream()
        .collect(
            Collectors.toUnmodifiableMap(entry -> entry.getKey().getData(), Map.Entry::getValue));
  }

  private CacheLoader<ContextualKey<String>, Optional<ServiceIdentifierEntity>>
      buildServiceEntitiesCacheLoader() {
    return new CacheLoader<>() {
      @Override
      public Optional<ServiceIdentifierEntity> load(ContextualKey<String> key) {
        return getServiceIdentifierEntities(key);
      }

      @Override
      public Map<ContextualKey<String>, Optional<ServiceIdentifierEntity>> loadAll(
          Iterable<? extends ContextualKey<String>> keys) {
        return getServiceIdentifierEntities(keys);
      }
    };
  }

  private Optional<ServiceIdentifierEntity> getServiceIdentifierEntities(
      ContextualKey<String> key) {
    return this.entityQueryServiceClient.getServiceEntities(List.of(key)).get(key);
  }

  private Map<ContextualKey<String>, Optional<ServiceIdentifierEntity>>
      getServiceIdentifierEntities(Iterable<? extends ContextualKey<String>> keys) {
    return this.entityQueryServiceClient.getServiceEntities(keys);
  }
}
