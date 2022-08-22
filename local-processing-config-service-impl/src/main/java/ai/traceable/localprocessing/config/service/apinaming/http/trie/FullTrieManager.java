package ai.traceable.localprocessing.config.service.apinaming.http.trie;

import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.SegmentConverter;
import ai.traceable.localprocessing.config.service.v1.ApiNamingPattern;
import ai.traceable.localprocessing.config.service.v1.FullPattern;
import ai.traceable.platform.apientity.Segment;
import ai.traceable.platform.apientity.TrieNodeType;
import ai.traceable.platform.apientity.http.model.TrieModel;
import ai.traceable.platform.apientity.http.model.TrieNodeConfig;
import ai.traceable.platform.deepstore.FileMetadata;
import ai.traceable.platform.model.PersistedModel;
import ai.traceable.platform.model.store.ModelPersistentStore;
import ai.traceable.platform.model.store.ServiceScope;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
public class FullTrieManager {

  private static final String CACHE_REFRESH_DURATION =
      "api.naming.config.trieModels.cache.refreshAfterWriteDuration";
  private static final String CACHE_EXPIRATION_DURATION =
      "api.naming.config.trieModels.cache.expireAfterWriteDuration";
  private static final String MAXIMUM_CACHE_SIZE = "api.naming.config.trieModels.cache.maximumSize";
  private static final String CACHE_NAME = "trieModelCache";
  private static final Duration CACHE_REFRESH_DURATION_DEFAULT = Duration.ofSeconds(150);
  private static final Duration CACHE_EXPIRATION_DURATION_DEFAULT = Duration.ofSeconds(300);
  private static final long MAXIMUM_CACHE_SIZE_DEFAULT = 100;

  private final LoadingCache<ContextualKey<ServiceScope>, Optional<TrieModel>> trieModelCache;
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

    this.trieModelStore = trieModelStore;
    this.segmentConverter = segmentConverter;
    this.trieModelCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheRefreshDuration.toMillis(), TimeUnit.MILLISECONDS)
            .expireAfterWrite(cacheExpiryDuration.toMillis(), TimeUnit.MILLISECONDS)
            .maximumSize(maximumCacheSize)
            .recordStats()
            .build(CacheLoader.from(this::loadTrieModel));
    PlatformMetricsRegistry.registerCache(CACHE_NAME, trieModelCache, Collections.emptyMap());
  }

  private Optional<TrieModel> loadTrieModel(@Nonnull ContextualKey<ServiceScope> key) {
    try {
      PersistedModel<TrieModel> persistedModel = trieModelStore.loadModel(key.getData());
      if (persistedModel == null) {
        return Optional.empty();
      }
      return Optional.ofNullable(persistedModel.getModel());
    } catch (Exception e) {
      log.error(
          "Could not fetch trie model for request context:{} and service scope:{} with exception: {}",
          key.getContext(),
          key.getData(),
          e);
      return Optional.empty();
    }
  }

  private Optional<TrieModel> getTrieModel(
      RequestContext requestContext, ServiceScope serviceScope) {
    try {
      return trieModelCache.get(requestContext.buildInternalContextualKey(serviceScope));
    } catch (Exception e) {
      log.error(
          "Unable to get trie model for request context:{}, service scope:{} with exception:{}",
          requestContext,
          serviceScope,
          e);
      return Optional.empty();
    }
  }

  public long getModelTimestamp(ServiceScope serviceScope) throws IOException {
    FileMetadata modelMetadata = trieModelStore.getModelMetadata(serviceScope);
    if (modelMetadata == null) {
      return 0;
    }
    return modelMetadata.getModificationTime();
  }

  public Optional<FullPattern> getFullPattern(
      RequestContext requestContext,
      ServiceScope serviceScope,
      HttpApiNamingConfigInfo httpApiNamingConfigInfo) {
    Optional<TrieModel> trieModelMaybe = getTrieModel(requestContext, serviceScope);
    if (trieModelMaybe.isEmpty()) {
      return Optional.empty();
    }
    List<List<Segment>> nonEmbryonicPaths =
        trieModelMaybe
            .get()
            .getNonEmbryonicWildcardPaths(
                buildTrieNodeConfig(httpApiNamingConfigInfo),
                httpApiNamingConfigInfo.getMaxNumberOfTriePaths());
    return Optional.of(
        buildFullPattern(nonEmbryonicPaths, httpApiNamingConfigInfo.getWildcardConfigMap()));
  }

  private FullPattern buildFullPattern(
      List<List<Segment>> paths, EnumMap<TrieNodeType, String> wildcardConfigMap) {
    FullPattern.Builder fullPatternBuilder = FullPattern.newBuilder();
    for (List<Segment> path : paths) {
      fullPatternBuilder.addApiNamingPatterns(convertPath(path, wildcardConfigMap));
    }
    return fullPatternBuilder.build();
  }

  private ApiNamingPattern convertPath(
      List<Segment> path, EnumMap<TrieNodeType, String> wildcardConfigMap) {
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
        List.of(httpApiNamingConfigInfo.getWildcardConfigMap().get(TrieNodeType.ID)),
        List.of(httpApiNamingConfigInfo.getWildcardConfigMap().get(TrieNodeType.LOW_CARDINALITY)),
        List.of(httpApiNamingConfigInfo.getWildcardConfigMap().get(TrieNodeType.HIGH_CARDINALITY)),
        new HashSet<>(httpApiNamingConfigInfo.getExtensions()),
        httpApiNamingConfigInfo.getEmbryonicThreshold());
  }
}
