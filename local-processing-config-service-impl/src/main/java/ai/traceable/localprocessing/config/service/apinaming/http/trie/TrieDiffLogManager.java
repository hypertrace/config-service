package ai.traceable.localprocessing.config.service.apinaming.http.trie;

import static java.util.function.Predicate.not;

import ai.traceable.localprocessing.config.service.apinaming.http.utils.SegmentConverter;
import ai.traceable.localprocessing.config.service.config.http.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.v1.TrieDiffLog;
import ai.traceable.localprocessing.config.service.v1.TrieNodePath;
import ai.traceable.platform.apientity.Addition;
import ai.traceable.platform.apientity.Deletion;
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
      "api.naming.config.trieDiffLog.cache.maximumCacheSize";
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

  public List<TrieDiffLog> getAllTrieDiffLogs(
      RequestContext requestContext, ModelScope scope, long agentTimestampMillis)
      throws ExecutionException {
    return getAllTrieDiffLogs(getTrieDiffLogModels(requestContext, scope, agentTimestampMillis));
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
    return Path.of(modelScopeAbsoluteDirPath).toAbsolutePath().toString();
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

  private List<TrieDiffLog> getAllTrieDiffLogs(
      List<PersistedModel<TrieDiffLogModel>> persistedModels) {
    return persistedModels.stream()
        .map(PersistedModel::getModel)
        .map(trieDiffLogModel -> getTrieDiffLogs(trieDiffLogModel.getTrieDiffLog()))
        .flatMap(List::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private List<TrieDiffLog> getTrieDiffLogs(
      ai.traceable.platform.apientity.TrieDiffLog trieDiffLog) {
    List<TrieDiffLog> pathAdditionTrieDiffLogs =
        convertPathAdditions(trieDiffLog.getPathAdditions());
    List<TrieDiffLog> nodeDeletionTrieDiffLogs =
        convertNodeDeletions(trieDiffLog.getNodeDeletions());
    return Streams.concat(pathAdditionTrieDiffLogs.stream(), nodeDeletionTrieDiffLogs.stream())
        .collect(Collectors.toUnmodifiableList());
  }

  private List<TrieDiffLog> convertPathAdditions(List<Addition> additions) {
    return additions.stream()
        .map(this::convertAdditionToValues)
        .filter(not(List::isEmpty))
        .map(
            values ->
                TrieDiffLog.newBuilder()
                    .setPathAddition(TrieNodePath.newBuilder().addAllValues(values).build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  private List<TrieDiffLog> convertNodeDeletions(List<Deletion> deletions) {
    return deletions.stream()
        .map(this::convertDeletionToValues)
        .filter(not(List::isEmpty))
        .map(
            values ->
                TrieDiffLog.newBuilder()
                    .setNodeRemoval(TrieNodePath.newBuilder().addAllValues(values).build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  private List<ai.traceable.localprocessing.config.service.v1.Value> convertAdditionToValues(
      Addition addition) {
    return addition.getSegments().stream()
        .map(segmentConverter::convertSegmentToValue)
        .collect(Collectors.toUnmodifiableList());
  }

  private List<ai.traceable.localprocessing.config.service.v1.Value> convertDeletionToValues(
      Deletion deletion) {
    return deletion.getSegments().stream()
        .map(segmentConverter::convertSegmentToValue)
        .collect(Collectors.toUnmodifiableList());
  }

  @Value
  private static class DiffLogIdentifier {
    String diffLogDirPath;
    ModelFilter filter;
  }
}
