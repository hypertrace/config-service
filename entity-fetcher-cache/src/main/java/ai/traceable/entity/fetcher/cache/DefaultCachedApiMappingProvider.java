package ai.traceable.entity.fetcher.cache;

import ai.traceable.entity.fetcher.cache.config.CachedEntityFetcherConfig;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Singleton
@Slf4j
class DefaultCachedApiMappingProvider implements CachedApiMappingProvider {
  private final EntityQueryServiceClient entityQueryServiceClient;
  private final LoadingCache<ContextualKey<String>, Optional<ApiIdentifierEntity>> apiEntitiesCache;
  private final LoadingCache<ContextualKey<String>, Set<ApiIdentifierEntity>>
      apiEntitiesHavingLabelsCache;

  @Inject
  DefaultCachedApiMappingProvider(
      EntityQueryServiceClient entityQueryServiceClient, CachedEntityFetcherConfig config) {
    this.entityQueryServiceClient = entityQueryServiceClient;
    this.apiEntitiesCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(config.getExpireAfterAccessDuration())
            .maximumSize(config.getMaxSize())
            .recordStats()
            .build(buildApiEntitiesCacheLoader());
    this.apiEntitiesHavingLabelsCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(config.getRefreshAfterWriteDuration())
            .expireAfterAccess(config.getExpireAfterAccessDuration())
            .maximumSize(config.getMaxSize())
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    buildApiEntitiesHavingLabelsCacheLoader(),
                    Executors.newSingleThreadExecutor()));
  }

  @SneakyThrows
  @Override
  public Map<String, Optional<ApiIdentifierEntity>> getApiIdentifierEntities(
      RequestContext requestContext, @NonNull Set<String> apiIds) {
    List<ContextualKey<String>> contextualKeys =
        apiIds.stream()
            .map(requestContext::buildInternalContextualKey)
            .collect(Collectors.toUnmodifiableList());
    ImmutableMap<ContextualKey<String>, Optional<ApiIdentifierEntity>> cachedValues =
        apiEntitiesCache.getAll(contextualKeys);
    return cachedValues.entrySet().stream()
        .collect(
            Collectors.toUnmodifiableMap(entry -> entry.getKey().getData(), Map.Entry::getValue));
  }

  @SneakyThrows
  @Override
  public Map<String, Set<ApiIdentifierEntity>> getApiIdentifierEntitiesHavingLabels(
      RequestContext requestContext, @NonNull Set<String> apiLabelIds) {
    List<ContextualKey<String>> contextualKeys =
        apiLabelIds.stream()
            .map(requestContext::buildInternalContextualKey)
            .collect(Collectors.toUnmodifiableList());
    ImmutableMap<ContextualKey<String>, Set<ApiIdentifierEntity>> cachedValues =
        apiEntitiesHavingLabelsCache.getAll(contextualKeys);
    return cachedValues.entrySet().stream()
        .collect(
            Collectors.toUnmodifiableMap(entry -> entry.getKey().getData(), Map.Entry::getValue));
  }

  private CacheLoader<ContextualKey<String>, Optional<ApiIdentifierEntity>>
      buildApiEntitiesCacheLoader() {
    return new CacheLoader<>() {
      @Override
      public Optional<ApiIdentifierEntity> load(ContextualKey<String> key) {
        return getApiIdentifierEntities(key);
      }

      @Override
      public Map<ContextualKey<String>, Optional<ApiIdentifierEntity>> loadAll(
          Iterable<? extends ContextualKey<String>> keys) {
        return getApiIdentifierEntities(keys);
      }
    };
  }

  private CacheLoader<ContextualKey<String>, Set<ApiIdentifierEntity>>
      buildApiEntitiesHavingLabelsCacheLoader() {
    return new CacheLoader<>() {
      @Override
      public Set<ApiIdentifierEntity> load(ContextualKey<String> key) {
        return getApiEntitiesHavingLabels(key);
      }

      @Override
      public Map<ContextualKey<String>, Set<ApiIdentifierEntity>> loadAll(
          Iterable<? extends ContextualKey<String>> keys) {
        return getApiEntitiesHavingLabels(keys);
      }

      @Override
      public ListenableFuture<Set<ApiIdentifierEntity>> reload(
          ContextualKey<String> key, Set<ApiIdentifierEntity> oldValue) throws Exception {
        try {
          return Futures.immediateFuture(getApiEntitiesHavingLabels(key));
        } catch (Exception ex) {
          return Futures.immediateFuture(oldValue);
        }
      }
    };
  }

  private Optional<ApiIdentifierEntity> getApiIdentifierEntities(ContextualKey<String> key) {
    return this.entityQueryServiceClient.getApiEntities(List.of(key)).get(key);
  }

  private Map<ContextualKey<String>, Optional<ApiIdentifierEntity>> getApiIdentifierEntities(
      Iterable<? extends ContextualKey<String>> keys) {
    return this.entityQueryServiceClient.getApiEntities(keys);
  }

  private Set<ApiIdentifierEntity> getApiEntitiesHavingLabels(ContextualKey<String> key) {
    return this.entityQueryServiceClient.getApiEntitiesHavingLabels(List.of(key)).get(key);
  }

  private Map<ContextualKey<String>, Set<ApiIdentifierEntity>> getApiEntitiesHavingLabels(
      Iterable<? extends ContextualKey<String>> keys) {
    return this.entityQueryServiceClient.getApiEntitiesHavingLabels(keys);
  }
}
