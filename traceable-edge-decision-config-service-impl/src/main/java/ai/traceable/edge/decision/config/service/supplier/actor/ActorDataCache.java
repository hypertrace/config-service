package ai.traceable.edge.decision.config.service.supplier.actor;

import ai.traceable.edge.decision.config.service.supplier.actor.config.ActorServiceConfig;
import ai.traceable.edge.decision.config.service.supplier.actor.config.CacheConfig;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Singleton
public class ActorDataCache {
  private static final String ACTOR_DATA_CACHE = "ActorDataCache";

  private final LoadingCache<ContextualKey<Optional<String>>, List<ActorData>> userCache;
  private final ActorStore actorStore;
  private final Clock clock;

  @Inject
  public ActorDataCache(ActorServiceConfig actorServiceConfig, ActorStore actorStore, Clock clock) {
    this.actorStore = actorStore;
    this.clock = clock;
    CacheConfig cacheConfig = actorServiceConfig.getCacheConfig();
    userCache =
        CacheBuilder.newBuilder()
            .maximumSize(cacheConfig.getMaxCacheSize())
            .expireAfterWrite(cacheConfig.getWriteExpirationDuration())
            .refreshAfterWrite(cacheConfig.getRefreshExpirationDuration())
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(this::loadValue), Executors.newSingleThreadExecutor()));
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        ACTOR_DATA_CACHE, userCache, Collections.emptyMap(), cacheConfig.getMaxCacheSize());
  }

  @SneakyThrows
  public List<ActorData> getActorData(ContextualKey<Optional<String>> contextualKey) {
    return Optional.of(userCache.get(contextualKey))
        .map(
            actorData ->
                actorData.stream()
                    .filter(actor -> actor.isActive(clock))
                    .collect(Collectors.toUnmodifiableList()))
        .orElse(Collections.emptyList());
  }

  private List<ActorData> loadValue(ContextualKey<Optional<String>> contextualKey) {
    RequestContext requestContext = contextualKey.getContext();
    Optional<String> environmentId = contextualKey.getData();
    return actorStore.getActiveThreatActorsWithUserId(requestContext, environmentId);
  }
}
