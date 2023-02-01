package ai.traceable.localprocessing.config.service.apinaming.http.trie;

import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.SegmentConverter;
import ai.traceable.localprocessing.config.service.v1.ApiNamingPattern;
import ai.traceable.localprocessing.config.service.v1.FullPattern;
import ai.traceable.platform.apientity.Segment;
import ai.traceable.platform.apientity.http.model.NodeType;
import ai.traceable.platform.apientity.http.model.TrieModel;
import ai.traceable.platform.apientity.http.model.TrieNodeConfig;
import ai.traceable.platform.deepstore.FileMetadata;
import ai.traceable.platform.model.PersistedModel;
import ai.traceable.platform.model.store.ModelPersistentStore;
import ai.traceable.platform.model.store.scope.ServiceScope;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

/**
 * The cache is not useful at all in case of 1TPA. Every subsequent request would be cache miss and
 * would be loaded. However it would be useful in case of multiple TPAs polling for the same service
 * (which would mostly arise only in daemonset mirroring kind of setups)
 *
 * <p>For the first agent requesting, each request would be a new load. For all the other agents
 * with the same request, it would be returned from the cache
 */
@Slf4j
public class FullTrieManager {

  private static final String CACHE_REFRESH_DURATION =
      "api.naming.config.trieModels.cache.refreshAfterWriteDuration";
  private static final String CACHE_EXPIRATION_DURATION =
      "api.naming.config.trieModels.cache.expireAfterWriteDuration";
  private static final String MAXIMUM_CACHE_SIZE = "api.naming.config.trieModels.cache.maximumSize";
  private static final String FULL_TRIE_CACHE_THREAD_POOL_SIZE =
      "api.naming.config.trieModels.cache.threadPoolSize";
  private static final String CACHE_NAME = "trieModelCache";
  private static final Duration CACHE_REFRESH_DURATION_DEFAULT = Duration.ofMinutes(30);
  private static final Duration CACHE_EXPIRATION_DURATION_DEFAULT = Duration.ofMinutes(60);
  private static final long MAXIMUM_CACHE_SIZE_DEFAULT = 100;
  private static final int FULL_TRIE_CACHE_THREAD_POOL_SIZE_DEFAULT = 2;

  private final LoadingCache<ContextualKey<FullTrieIdentifier>, FullTrieData> trieModelCache;
  private final ModelPersistentStore<TrieModel> trieModelStore;
  private final SegmentConverter segmentConverter;

  @Inject
  public FullTrieManager(
      Config config,
      ModelPersistentStore<TrieModel> trieModelStore,
      SegmentConverter segmentConverter) {
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
    int fullTrieCacheThreadPoolSize =
        config.hasPath(FULL_TRIE_CACHE_THREAD_POOL_SIZE)
            ? config.getInt(FULL_TRIE_CACHE_THREAD_POOL_SIZE)
            : FULL_TRIE_CACHE_THREAD_POOL_SIZE_DEFAULT;

    this.trieModelStore = trieModelStore;
    this.segmentConverter = segmentConverter;
    this.trieModelCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheRefreshDuration.toMillis(), TimeUnit.MILLISECONDS)
            .expireAfterWrite(cacheExpiryDuration.toMillis(), TimeUnit.MILLISECONDS)
            .maximumSize(maximumCacheSize)
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(this::loadTrieModelData),
                    Executors.newFixedThreadPool(
                        fullTrieCacheThreadPoolSize, this.buildFullTrieCacheThreadFactory())));
    PlatformMetricsRegistry.registerCache(CACHE_NAME, trieModelCache, Collections.emptyMap());
  }

  private FullTrieData loadTrieModelData(@Nonnull ContextualKey<FullTrieIdentifier> key) {
    try {
      log.debug(
          "Loading trie model for request context: {}, FullTrieIdentifier:{}",
          key.getContext(),
          key.getData());
      PersistedModel<TrieModel> persistedModel =
          trieModelStore.loadModel(key.getData().getServiceScope());
      if (persistedModel == null) {
        log.debug(
            "Trie model loaded for request context: {}, serviceScope:{} is null",
            key.getContext(),
            key.getData());
        return new FullTrieData(Optional.empty(), 0);
      }
      return new FullTrieData(
          Optional.ofNullable(persistedModel.getModel()),
          persistedModel.getMetadata().getModificationTime());
    } catch (Exception e) {
      log.error(
          "Could not fetch trie model for request context:{} and service scope:{} with exception: {}",
          key.getContext(),
          key.getData(),
          e);
      return new FullTrieData(Optional.empty(), 0);
    }
  }

  public FullTrieData getTrieModelData(
      RequestContext requestContext, ServiceScope serviceScope, long agentTimestampMillis) {
    try {
      log.debug(
          "Fetching trie model for request context: {}, serviceScope:{}",
          requestContext,
          serviceScope);
      return trieModelCache.get(
          requestContext.buildInternalContextualKey(
              new FullTrieIdentifier(serviceScope, agentTimestampMillis)));
    } catch (Exception e) {
      log.error(
          "Unable to get trie model for request context:{}, service scope:{} with exception:{}",
          requestContext,
          serviceScope,
          e);
      return new FullTrieData(Optional.empty(), 0);
    }
  }

  public long getModelTimestamp(ServiceScope serviceScope) throws IOException {
    FileMetadata modelMetadata = trieModelStore.getModelMetadata(serviceScope);
    if (modelMetadata == null) {
      return 0;
    }
    return modelMetadata.getModificationTime();
  }

  public FullPattern getFullPattern(
      TrieModel trieModel, HttpApiNamingConfigInfo httpApiNamingConfigInfo) {
    List<List<Segment>> nonEmbryonicPaths =
        trieModel.getNonEmbryonicWildcardPaths(
            buildTrieNodeConfig(httpApiNamingConfigInfo),
            httpApiNamingConfigInfo.getMaxNumberOfTriePaths());
    return buildFullPattern(nonEmbryonicPaths, httpApiNamingConfigInfo.getWildcardConfigMap());
  }

  private FullPattern buildFullPattern(
      List<List<Segment>> paths, EnumMap<NodeType, String> wildcardConfigMap) {
    FullPattern.Builder fullPatternBuilder = FullPattern.newBuilder();
    for (List<Segment> path : paths) {
      fullPatternBuilder.addApiNamingPatterns(convertPath(path, wildcardConfigMap));
    }
    return fullPatternBuilder.build();
  }

  private ApiNamingPattern convertPath(
      List<Segment> path, EnumMap<NodeType, String> wildcardConfigMap) {
    ApiNamingPattern.Builder apiNamingPatternBuilder = ApiNamingPattern.newBuilder();
    path.forEach(
        segment ->
            apiNamingPatternBuilder.addSegments(
                segmentConverter.convertSegment(segment, wildcardConfigMap)));
    return apiNamingPatternBuilder.build();
  }

  private TrieNodeConfig buildTrieNodeConfig(HttpApiNamingConfigInfo httpApiNamingConfigInfo) {
    return new TrieNodeConfig(
        httpApiNamingConfigInfo.getSegmentWhitelistRegexes(),
        List.of(httpApiNamingConfigInfo.getWildcardConfigMap().get(NodeType.ID)),
        List.of(httpApiNamingConfigInfo.getWildcardConfigMap().get(NodeType.LOW_CARDINALITY)),
        List.of(httpApiNamingConfigInfo.getWildcardConfigMap().get(NodeType.HIGH_CARDINALITY)),
        new HashSet<>(httpApiNamingConfigInfo.getExtensions()),
        httpApiNamingConfigInfo.getEmbryonicThreshold());
  }

  private ThreadFactory buildFullTrieCacheThreadFactory() {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat("full-trie-cache-%d").build();
  }

  @Value
  private static class FullTrieIdentifier {
    ServiceScope serviceScope;
    long agentTimestampMillis;
  }

  @Value
  static class FullTrieData {
    Optional<TrieModel> trieModelMaybe;
    long trieModelTimestampMillis;
  }
}
