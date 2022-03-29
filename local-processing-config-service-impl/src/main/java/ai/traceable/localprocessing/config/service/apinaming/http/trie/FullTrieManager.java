package ai.traceable.localprocessing.config.service.apinaming.http.trie;

import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.SegmentConverter;
import ai.traceable.localprocessing.config.service.v1.FullTrie;
import ai.traceable.localprocessing.config.service.v1.Node;
import ai.traceable.localprocessing.config.service.v1.WildcardConfig;
import ai.traceable.localprocessing.config.service.v1.WildcardType;
import ai.traceable.platform.apientity.Segment;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
  private static final String MAXIMUM_CACHE_SIZE =
      "api.naming.config.trieModels.cache.maximumCacheSize";
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

  public Optional<FullTrie> getFullTrie(
      RequestContext requestContext,
      ServiceScope serviceScope,
      HttpApiNamingConfigInfo httpApiNamingConfigInfo) {
    Optional<TrieModel> trieModelMaybe = getTrieModel(requestContext, serviceScope);
    if (trieModelMaybe.isEmpty()) {
      return Optional.empty();
    }
    Set<List<Segment>> nonEmbryonicPaths =
        trieModelMaybe.get().getNonEmbryonicPaths(buildTrieNodeConfig(httpApiNamingConfigInfo));
    return Optional.of(buildFullTrie(nonEmbryonicPaths));
  }

  private FullTrie buildFullTrie(Set<List<Segment>> paths) {
    ArrayList<Node> roots = new ArrayList<>();
    for (List<Segment> segments : paths) {
      roots = insertIntoTrie(roots, segments, 0);
    }
    return FullTrie.newBuilder().addAllRoots(roots).build();
  }

  private ArrayList<Node> insertIntoTrie(ArrayList<Node> roots, List<Segment> segments, int index) {
    if (segments.size() == index) {
      return new ArrayList<>();
    }

    ai.traceable.localprocessing.config.service.v1.Value segmentValue =
        segmentConverter.convertSegmentToValue(segments.get(index));
    Optional<Node> maybeNode = getNode(roots, segmentValue);
    if (maybeNode.isPresent()) {
      Node currentNode = maybeNode.get();
      Node newNode =
          Node.newBuilder(currentNode)
              .clearChildren()
              .addAllChildren(
                  insertIntoTrie(
                      new ArrayList<>(currentNode.getChildrenList()), segments, index + 1))
              .build();
      roots.remove(currentNode);
      roots.add(newNode);
    } else {
      roots.add(
          Node.newBuilder()
              .setValue(segmentValue)
              .addAllChildren(insertIntoTrie(new ArrayList<>(), segments, index + 1))
              .build());
    }
    return roots;
  }

  private Optional<Node> getNode(
      List<Node> nodes, ai.traceable.localprocessing.config.service.v1.Value segmentValue) {
    return nodes.stream().filter(node -> segmentValue.equals(node.getValue())).findFirst();
  }

  private TrieNodeConfig buildTrieNodeConfig(HttpApiNamingConfigInfo httpApiNamingConfigInfo) {
    ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig httpApiNamingConfig =
        httpApiNamingConfigInfo.getHttpApiNamingConfig();
    return new TrieNodeConfig(
        httpApiNamingConfig.getSegmentWhitelistRegexesList(),
        getWildcardConfigList(
            filterWildcardConfigs(
                httpApiNamingConfig.getWildcardConfigsList(), WildcardType.WILDCARD_TYPE_ID)),
        getWildcardConfigList(
            filterWildcardConfigs(
                httpApiNamingConfig.getWildcardConfigsList(),
                WildcardType.WILDCARD_TYPE_LOW_CARDINALITY)),
        getWildcardConfigList(
            filterWildcardConfigs(
                httpApiNamingConfig.getWildcardConfigsList(),
                WildcardType.WILDCARD_TYPE_HIGH_CARDINALITY)),
        new HashSet<>(httpApiNamingConfig.getExtensionsList()),
        httpApiNamingConfigInfo.getEmbryonicThreshold());
  }

  private Optional<List<String>> filterWildcardConfigs(
      List<WildcardConfig> wildcardConfigs, WildcardType wildcardType) {
    return wildcardConfigs.stream()
        .filter(wildcardConfig -> wildcardConfig.getWildcardType().equals(wildcardType))
        .map(WildcardConfig::getIdentificationRegexesList)
        .map(List::copyOf)
        .findAny();
  }

  private List<String> getWildcardConfigList(Optional<List<String>> protocolStringListOptional) {
    if (protocolStringListOptional.isEmpty()) {
      return Collections.emptyList();
    }
    return protocolStringListOptional.get();
  }
}
