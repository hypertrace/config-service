package ai.traceable.aiapp.protection.config.service.firewall.cache;

import ai.traceable.aiapp.protection.config.service.firewall.AiAppConfigServiceConfig;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppEvaluationConfigContextRequest;
import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.protection.engine.config.aifirewall.v1.AiFirewallConfigContext;
import ai.traceable.protection.rules.aiapp.v1.AiAppRulesProvider;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
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

@Singleton
@Slf4j
public class AiAppConfigContextCacheProvider extends AiAppConfigContextClientProvider {
  private static final String CACHE_NAME = "aiAppConfigContextCache";
  private final LoadingCache<ContextualKey<AiAppConfigContextKey>, AiFirewallConfigContext> cache;

  @Inject
  public AiAppConfigContextCacheProvider(
      AiAppConfigServiceConfig config,
      KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener,
      AnomalyDetectionConfigManager anomalyDetectionConfigManager,
      AiAppRulesProvider aiAppRulesProvider,
      CachedServiceMappingProvider cachedServiceMappingProvider,
      CachedApiMappingProvider cachedApiMappingProvider) {
    super(
        anomalyDetectionConfigManager,
        aiAppRulesProvider,
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
  public AiFirewallConfigContext getAiFirewallConfigContext(
      RequestContext requestContext, GetAiAppEvaluationConfigContextRequest request) {
    ContextualKey<AiAppConfigContextKey> cacheKey =
        requestContext.buildInternalContextualKey(AiAppConfigContextKey.from(request));
    try {
      return cache.get(cacheKey);
    } catch (ExecutionException e) {
      log.error(
          "Error loading from cache for tenant: {}, returning default instance",
          requestContext.getTenantId(),
          e);
      return AiFirewallConfigContext.getDefaultInstance();
    }
  }

  private LoadingCache<ContextualKey<AiAppConfigContextKey>, AiFirewallConfigContext> buildCache(
      AiAppConfigServiceConfig aiAppConfigServiceConfig) {
    return CacheBuilder.newBuilder()
        .maximumSize(aiAppConfigServiceConfig.getAiAppConfigContextCacheMaxSize())
        .refreshAfterWrite(
            aiAppConfigServiceConfig.getAiAppConfigContextCacheRefreshAfterWriteDuration())
        .recordStats()
        .build(
            CacheLoader.asyncReloading(
                createCacheLoader(),
                Executors.newFixedThreadPool(
                    aiAppConfigServiceConfig.getAiAppConfigContextCacheThreadPoolSize(),
                    new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("ai-app-config-ctxt-cache-%d")
                        .build())));
  }

  private CacheLoader<ContextualKey<AiAppConfigContextKey>, AiFirewallConfigContext>
      createCacheLoader() {
    return new CacheLoader<>() {

      @Override
      public AiFirewallConfigContext load(ContextualKey<AiAppConfigContextKey> key) {
        return loadAiFirewallConfigContext(key.getContext(), key.getData().getRequest());
      }

      @Override
      public ListenableFuture<AiFirewallConfigContext> reload(
          ContextualKey<AiAppConfigContextKey> cacheKey, AiFirewallConfigContext existingValue) {
        try {
          return Futures.immediateFuture(
              loadAiFirewallConfigContext(cacheKey.getContext(), cacheKey.getData().getRequest()));
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

  private void registerCacheMetrics(AiAppConfigServiceConfig aiAppConfigServiceConfig) {
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        CACHE_NAME,
        this.cache,
        Collections.emptyMap(),
        aiAppConfigServiceConfig.getAiAppConfigContextCacheMaxSize());
  }

  private void handleConfigChangeEvent(ConfigChangeEventKey key, ConfigChangeEventValue value) {
    if (!key.getConfigType().equals(ScopedAnomalyDetectionConfig.class.getName())
        && !key.getConfigType().equals(ScopedAnomalyConfigStatusChange.class.getName())) {
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
