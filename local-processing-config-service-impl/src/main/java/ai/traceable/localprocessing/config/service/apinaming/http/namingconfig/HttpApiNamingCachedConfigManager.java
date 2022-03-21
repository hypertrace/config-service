package ai.traceable.localprocessing.config.service.apinaming.http.namingconfig;

import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetTrainingConfigsFilter;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainerConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
public class HttpApiNamingCachedConfigManager {
  private static final String CACHE_REFRESH_DURATION =
      "api.naming.config.configs.cache.refreshAfterWriteDuration";
  private static final String CACHE_EXPIRATION_DURATION =
      "api.naming.config.configs.cache.expireAfterWriteDuration";
  private static final String MAXIMUM_CACHE_SIZE =
      "api.naming.config.configs.cache.maximumCacheSize";
  private static final String CACHE_NAME = "trainingConfigCache";
  private static final Duration CACHE_REFRESH_DURATION_DEFAULT = Duration.ofHours(12);
  private static final Duration CACHE_EXPIRATION_DURATION_DEFAULT = Duration.ofHours(24);
  private static final long MAXIMUM_CACHE_SIZE_DEFAULT = 100;

  private final LoadingCache<ContextualKey<Void>, List<ScopedTrainingConfig>> trainingConfigCache;
  private final TrainerConfigServiceGrpc.TrainerConfigServiceBlockingStub
      trainerConfigServiceBlockingStub;

  @Inject
  public HttpApiNamingCachedConfigManager(
      Config config,
      TrainerConfigServiceGrpc.TrainerConfigServiceBlockingStub trainerConfigServiceBlockingStub) {
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

    this.trainerConfigServiceBlockingStub = trainerConfigServiceBlockingStub;
    this.trainingConfigCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheRefreshDuration.toMillis(), TimeUnit.MILLISECONDS)
            .expireAfterWrite(cacheExpiryDuration.toMillis(), TimeUnit.MILLISECONDS)
            .maximumSize(maximumCacheSize)
            .recordStats()
            .build(CacheLoader.from(this::loadScopedTrainingConfigs));
    PlatformMetricsRegistry.registerCache(CACHE_NAME, trainingConfigCache, Collections.emptyMap());
  }

  private List<ScopedTrainingConfig> loadScopedTrainingConfigs(@Nonnull ContextualKey<Void> key) {
    try {
      return key.callInContext(
              () ->
                  trainerConfigServiceBlockingStub.getAllScopedTrainingConfigs(
                      buildGetAllScopedTrainingConfigsRequest()))
          .getScopedTrainingConfigsList();
    } catch (Exception e) {
      log.error("Could not fetch training configs. Request context:{}", key.getContext(), e);
      return Collections.emptyList();
    }
  }

  private List<ScopedTrainingConfig> get(RequestContext requestContext) throws ExecutionException {
    return trainingConfigCache.get(requestContext.buildInternalContextualKey());
  }

  private GetAllScopedTrainingConfigsRequest buildGetAllScopedTrainingConfigsRequest() {
    return GetAllScopedTrainingConfigsRequest.newBuilder()
        .setFilter(
            GetTrainingConfigsFilter.newBuilder()
                .addTrainingConfigTypes(TrainingConfigType.TRAINING_CONFIG_TYPE_API_NAMING))
        .build();
  }

  public Optional<List<TrainingConfig>> getTrainingConfigListForService(
      RequestContext requestContext, String serviceId) throws ExecutionException {
    List<ScopedTrainingConfig> scopedTrainingConfigs = get(requestContext);
    try {
      return scopedTrainingConfigs.stream()
          .filter(scopedTrainingConfig -> scopedTrainingConfig.getConfigScope().hasServiceScope())
          .filter(
              scopedTrainingConfig ->
                  scopedTrainingConfig.getConfigScope().getServiceScope().getId().equals(serviceId))
          .findAny()
          .map(ScopedTrainingConfig::getTrainingConfigsList)
          .or(() -> getTrainingConfigListForTenant(scopedTrainingConfigs, requestContext));
    } catch (Exception exception) {
      throw new RuntimeException(
          String.format(
              "Get Training Config List failed for request context %s and serviceId:%s with exception %s",
              requestContext, serviceId, exception));
    }
  }

  private Optional<List<TrainingConfig>> getTrainingConfigListForTenant(
      List<ScopedTrainingConfig> scopedTrainingConfigs, RequestContext requestContext) {
    try {
      return scopedTrainingConfigs.stream()
          .filter(scopedTrainingConfig -> scopedTrainingConfig.getConfigScope().hasCustomerScope())
          .findAny()
          .map(ScopedTrainingConfig::getTrainingConfigsList);
    } catch (Exception exception) {
      log.error(
          "Get Training Config List failed for request context {} with exception {}",
          requestContext,
          exception);
      return Optional.empty();
    }
  }
}
