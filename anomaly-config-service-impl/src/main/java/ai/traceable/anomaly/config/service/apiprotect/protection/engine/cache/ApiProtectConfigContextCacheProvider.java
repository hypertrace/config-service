package ai.traceable.anomaly.config.service.apiprotect.protection.engine.cache;

import ai.traceable.anomaly.config.service.apiprotect.ApiProtectConfigServiceConfig;
import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextRequest;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionConfigContext;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectionRulesProvider;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
public class ApiProtectConfigContextCacheProvider extends ApiProtectConfigContextClientProvider {
  private static final String CACHE_NAME = "apiProtectConfigContextCache";

  private final LoadingCache<ContextualKey<ApiProtectConfigContextKey>, ApiProtectionConfigContext>
      cache;

  @Inject
  public ApiProtectConfigContextCacheProvider(
      ApiProtectConfigServiceConfig config,
      KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener,
      FeatureCachingClient featureCachingClient,
      AnomalyDetectionConfigManager anomalyDetectionConfigManager,
      GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager,
      ApiProtectionRulesProvider apiProtectionRulesProvider,
      CachedServiceMappingProvider cachedServiceMappingProvider,
      CachedApiMappingProvider cachedApiMappingProvider) {
    super(
        featureCachingClient,
        anomalyDetectionConfigManager,
        globalAnomalyConfigStatusManager,
        apiProtectionRulesProvider,
        cachedServiceMappingProvider,
        cachedApiMappingProvider);
    this.cache = buildCache(config);
    try {
      kafkaLiveEventListener.registerCallback(this::handleConfigChangeEvent);
    } catch (Exception e) {
      log.warn(
          "Failed to register Kafka event listener for {}, cache invalidation will not work",
          CACHE_NAME,
          e);
    }
    registerCacheMetrics(config);
  }

  @Override
  public ApiProtectionConfigContext getApiProtectionConfigContext(
      RequestContext requestContext, GetApiProtectEvaluationConfigContextRequest request) {
    ContextualKey<ApiProtectConfigContextKey> cacheKey =
        requestContext.buildInternalContextualKey(ApiProtectConfigContextKey.from(request));
    try {
      return cache.get(cacheKey);
    } catch (ExecutionException e) {
      log.error(
          "Error loading from cache for tenant: {}, returning default instance",
          requestContext.getTenantId(),
          e);
      return ApiProtectionConfigContext.getDefaultInstance();
    }
  }

  private LoadingCache<ContextualKey<ApiProtectConfigContextKey>, ApiProtectionConfigContext>
      buildCache(ApiProtectConfigServiceConfig apiProtectConfigServiceConfig) {
    return CacheBuilder.newBuilder()
        .maximumSize(apiProtectConfigServiceConfig.getApiProtectConfigContextCacheMaxSize())
        .refreshAfterWrite(
            apiProtectConfigServiceConfig
                .getApiProtectConfigContextCacheRefreshAfterWriteDuration())
        .recordStats()
        .build(
            CacheLoader.asyncReloading(
                createCacheLoader(),
                Executors.newFixedThreadPool(
                    apiProtectConfigServiceConfig.getApiProtectConfigContextCacheThreadPoolSize(),
                    new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("api-protect-config-ctxt-cache-%d")
                        .build())));
  }

  private CacheLoader<ContextualKey<ApiProtectConfigContextKey>, ApiProtectionConfigContext>
      createCacheLoader() {
    return new CacheLoader<>() {

      @Override
      public ApiProtectionConfigContext load(ContextualKey<ApiProtectConfigContextKey> key) {
        return loadApiProtectionConfigContext(key.getContext(), key.getData().getRequest());
      }

      @Override
      public ListenableFuture<ApiProtectionConfigContext> reload(
          ContextualKey<ApiProtectConfigContextKey> cacheKey,
          ApiProtectionConfigContext existingValue) {
        try {
          return Futures.immediateFuture(
              loadApiProtectionConfigContext(
                  cacheKey.getContext(), cacheKey.getData().getRequest()));
        } catch (Exception e) {
          log.error(
              "Failed to reload cache entry for tenant: {}",
              cacheKey.getContext().getTenantId(),
              e);
        }
        return Futures.immediateFuture(existingValue);
      }
    };
  }

  private void registerCacheMetrics(ApiProtectConfigServiceConfig apiProtectConfigServiceConfig) {
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        CACHE_NAME,
        this.cache,
        Collections.emptyMap(),
        apiProtectConfigServiceConfig.getApiProtectConfigContextCacheMaxSize());
  }

  private void handleConfigChangeEvent(ConfigChangeEventKey key, ConfigChangeEventValue value) {
    if (!key.getConfigType().equals(ScopedAnomalyDetectionConfig.class.getName())
        && !key.getConfigType().equals(ScopedAnomalyConfigStatus.class.getName())) {
      return;
    }
    switch (value.getEventCase()) {
      case CREATE_EVENT:
      case UPDATE_EVENT:
      case DELETE_EVENT:
        invalidateCacheForTenant(key.getTenantId());
        break;
      default:
        log.debug("Ignoring event type: {}", value.getEventCase());
    }
  }

  private void invalidateCacheForTenant(String tenantId) {
    cache
        .asMap()
        .keySet()
        .removeIf(
            key ->
                key.getContext().getTenantId().isPresent()
                    && key.getContext().getTenantId().get().equals(tenantId));
  }
}
