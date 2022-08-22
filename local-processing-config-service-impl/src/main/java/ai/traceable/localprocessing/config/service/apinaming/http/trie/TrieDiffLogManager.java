package ai.traceable.localprocessing.config.service.apinaming.http.trie;

import static java.util.function.Predicate.not;

import ai.traceable.localprocessing.config.service.apinaming.http.utils.SegmentConverter;
import ai.traceable.localprocessing.config.service.config.http.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.v1.ApiNamingPattern;
import ai.traceable.localprocessing.config.service.v1.DiffLog;
import ai.traceable.localprocessing.config.service.v1.Segment;
import ai.traceable.platform.apientity.Addition;
import ai.traceable.platform.apientity.Deletion;
import ai.traceable.platform.apientity.TrieDiffLog;
import ai.traceable.platform.apientity.TrieNodeType;
import ai.traceable.platform.apientity.http.difflog.TrieDiffLogModel;
import ai.traceable.platform.model.PersistedModel;
import ai.traceable.platform.model.filter.ModelFilter;
import ai.traceable.platform.model.store.ModelPersistentStore;
import ai.traceable.platform.model.store.ModelScope;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class TrieDiffLogManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TrieDiffLogManager.class);

  private static final String CACHE_REFRESH_DURATION =
      "api.naming.config.trieDiffLog.cache.refreshAfterWriteDuration";
  private static final String CACHE_EXPIRATION_DURATION =
      "api.naming.config.trieDiffLog.cache.expireAfterWriteDuration";
  private static final String MAXIMUM_CACHE_SIZE =
      "api.naming.config.trieDiffLog.cache.maximumSize";
  private static final String DIFF_LOG_DIRECTORY_NAME =
      "api.naming.config.trieDiffLog.directory.name";
  private static final String DIFF_LOG_DIRECTORY_DEFAULT_NAME = "difflog";
  private static final String CACHE_NAME = "diffLogsCache";
  private static final Duration CACHE_REFRESH_DURATION_DEFAULT = Duration.ofSeconds(150);
  private static final Duration CACHE_EXPIRATION_DURATION_DEFAULT = Duration.ofSeconds(300);
  private static final long MAXIMUM_CACHE_SIZE_DEFAULT = 100;

  private final LoadingCache<
          ContextualKey<DiffLogIdentifier>, List<PersistedModel<TrieDiffLogModel>>>
      diffLogsCache;
  private final HttpApiNamingConfig httpApiNamingConfig;
  private final SegmentConverter segmentConverter;
  private final ModelPersistentStore<TrieDiffLogModel> trieDiffLogModelStore;
  private final String diffLogDirectoryName;

  @Inject
  public TrieDiffLogManager(
      Config config,
      ModelPersistentStore<TrieDiffLogModel> trieDiffLogModelStore,
      HttpApiNamingConfig httpApiNamingConfig,
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

    this.trieDiffLogModelStore = trieDiffLogModelStore;
    this.httpApiNamingConfig = httpApiNamingConfig;
    this.segmentConverter = segmentConverter;
    this.diffLogDirectoryName =
        config.hasPath(DIFF_LOG_DIRECTORY_NAME)
            ? config.getString(DIFF_LOG_DIRECTORY_NAME)
            : DIFF_LOG_DIRECTORY_DEFAULT_NAME;
    this.diffLogsCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheExpiryDuration.toMillis(), TimeUnit.MILLISECONDS)
            .refreshAfterWrite(cacheRefreshDuration.toMillis(), TimeUnit.MILLISECONDS)
            .maximumSize(maximumCacheSize)
            .recordStats()
            .build(CacheLoader.from(this::loadTrieDiffLogModels));
    PlatformMetricsRegistry.registerCache(CACHE_NAME, diffLogsCache, Collections.emptyMap());
  }

  private List<PersistedModel<TrieDiffLogModel>> loadTrieDiffLogModels(
      @Nonnull ContextualKey<DiffLogIdentifier> key) {
    try {
      return retry(
          () ->
              trieDiffLogModelStore
                  .loadModelsInDir(key.getData().getDiffLogDirPath(), key.getData().getFilter())
                  .values()
                  .stream()
                  .collect(Collectors.toUnmodifiableList()));
    } catch (Exception e) {
      log.error(
          "Could not fetch diff logs for request context:{} and diff log identifier : {} with exception: {}",
          key.getContext(),
          key.getData(),
          e);
      return Collections.emptyList();
    }
  }

  private List<PersistedModel<TrieDiffLogModel>> getTrieDiffLogModels(
      RequestContext requestContext, ModelScope scope, long agentTimestampMillis)
      throws ExecutionException {
    String diffLogDirPath =
        this.getAbsoluteDiffLogDirPath(Path.of(httpApiNamingConfig.getBaseDirectory()), scope);
    return diffLogsCache.get(
        requestContext.buildInternalContextualKey(
            new DiffLogIdentifier(diffLogDirPath, buildModelFilter(agentTimestampMillis))));
  }

  public List<DiffLog> getAllTrieDiffLogs(
      RequestContext requestContext,
      ModelScope scope,
      long agentTimestampMillis,
      Map<TrieNodeType, String> wildcardConfigMap)
      throws ExecutionException {
    return getAllTrieDiffLogs(
        getTrieDiffLogModels(requestContext, scope, agentTimestampMillis), wildcardConfigMap);
  }

  public long getLatestDiffLogTimestamp(
      RequestContext requestContext, ModelScope scope, long agentTimestampMillis)
      throws ExecutionException {
    return getTrieDiffLogModels(requestContext, scope, agentTimestampMillis).stream()
        .map(persistedModel -> persistedModel.getMetadata().getModificationTime())
        .max(Long::compare)
        .orElse(0L);
  }

  private ModelFilter buildModelFilter(long startTimestamp) {
    return ModelFilter.builder().modifiedStartTimestamp(startTimestamp).build();
  }

  private String getAbsoluteDiffLogDirPath(Path baseDir, ModelScope scope) {
    String modelScopeSubPath = scope.getSubPath();
    String modelScopeAbsoluteDirPath =
        baseDir.resolve(modelScopeSubPath).toAbsolutePath().toString();
    return Path.of(modelScopeAbsoluteDirPath, this.diffLogDirectoryName)
        .toAbsolutePath()
        .toString();
  }

  private <T> T retry(Callable<T> callable) throws ExecutionException, RetryException {
    Retryer retryer =
        RetryerBuilder.newBuilder()
            .retryIfExceptionOfType(IOException.class)
            .withWaitStrategy(WaitStrategies.fixedWait(100L, TimeUnit.MILLISECONDS))
            .withStopStrategy(StopStrategies.stopAfterAttempt(2))
            .build();

    try {
      return (T) retryer.call(callable);
    } catch (ExecutionException | RetryException ex) {
      LOGGER.error("Error in loading model after retrying", ex);
      throw ex;
    }
  }

  private List<DiffLog> getAllTrieDiffLogs(
      List<PersistedModel<TrieDiffLogModel>> persistedModels,
      Map<TrieNodeType, String> wildcardConfigMap) {
    return persistedModels.stream()
        .map(PersistedModel::getModel)
        .map(
            trieDiffLogModel ->
                getTrieDiffLogs(trieDiffLogModel.getTrieDiffLog(), wildcardConfigMap))
        .flatMap(List::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private List<DiffLog> getTrieDiffLogs(
      TrieDiffLog trieDiffLog, Map<TrieNodeType, String> wildcardConfigMap) {
    List<DiffLog> pathAdditionTrieDiffLogs =
        convertPathAdditions(trieDiffLog.getPathAdditions(), wildcardConfigMap);
    List<DiffLog> nodeDeletionTrieDiffLogs =
        convertNodeDeletions(trieDiffLog.getNodeDeletions(), wildcardConfigMap);
    return Streams.concat(pathAdditionTrieDiffLogs.stream(), nodeDeletionTrieDiffLogs.stream())
        .collect(Collectors.toUnmodifiableList());
  }

  private List<DiffLog> convertPathAdditions(
      List<Addition> additions, Map<TrieNodeType, String> wildcardConfigMap) {
    return additions.stream()
        .map(addition -> convertAdditionToValues(addition, wildcardConfigMap))
        .filter(not(List::isEmpty))
        .map(
            segments ->
                DiffLog.newBuilder()
                    .setApiNamingPatternAddition(
                        ApiNamingPattern.newBuilder().addAllSegments(segments).build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  private List<DiffLog> convertNodeDeletions(
      List<Deletion> deletions, Map<TrieNodeType, String> wildcardConfigMap) {
    return deletions.stream()
        .map(deletion -> convertDeletionToValues(deletion, wildcardConfigMap))
        .filter(not(List::isEmpty))
        .map(
            segments ->
                DiffLog.newBuilder()
                    .setApiNamingPatternDeletion(
                        ApiNamingPattern.newBuilder().addAllSegments(segments).build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  private List<Segment> convertAdditionToValues(
      Addition addition, Map<TrieNodeType, String> wildcardConfigMap) {
    return addition.getSegments().stream()
        .map(segment -> segmentConverter.convertSegment(segment, wildcardConfigMap))
        .collect(Collectors.toUnmodifiableList());
  }

  private List<Segment> convertDeletionToValues(
      Deletion deletion, Map<TrieNodeType, String> wildcardConfigMap) {
    return deletion.getSegments().stream()
        .map(segment -> segmentConverter.convertSegment(segment, wildcardConfigMap))
        .collect(Collectors.toUnmodifiableList());
  }

  @Value
  private static class DiffLogIdentifier {
    String diffLogDirPath;
    ModelFilter filter;
  }
}
