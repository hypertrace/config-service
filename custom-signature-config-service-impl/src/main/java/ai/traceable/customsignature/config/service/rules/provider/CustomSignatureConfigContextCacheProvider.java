package ai.traceable.customsignature.config.service.rules.provider;

import ai.traceable.customsignature.config.service.CustomSignatureConfigServiceConfig;
import ai.traceable.customsignature.config.service.rules.CustomSignatureRulesStore;
import ai.traceable.customsignature.config.service.rules.converter.CustomSignatureConfigContextConverter;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextRequest;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConfigContext;
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
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Singleton
@Slf4j
public class CustomSignatureConfigContextCacheProvider
    extends CustomSignatureConfigContextClientProvider {

  private static final String CACHE_NAME = "customSignatureConfigContextCache";

  private final LoadingCache<
          ContextualKey<CustomSignatureConfigContextKey>, CustomSignatureConfigContext>
      cache;

  @Inject
  public CustomSignatureConfigContextCacheProvider(
      CustomSignatureRulesStore rulesStore,
      CustomSignatureConfigContextConverter configContextConverter,
      KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener,
      CustomSignatureConfigServiceConfig config) {
    super(rulesStore, configContextConverter);
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
  public CustomSignatureConfigContext getCustomSignatureConfigContext(
      RequestContext requestContext, GetCustomSignatureEvaluationConfigContextRequest request) {

    ContextualKey<CustomSignatureConfigContextKey> cacheKey =
        requestContext.buildInternalContextualKey(CustomSignatureConfigContextKey.from(request));
    try {
      return cache.get(cacheKey);
    } catch (ExecutionException e) {
      log.error(
          "Error loading from cache for tenant: {}, returning default instance",
          requestContext.getTenantId(),
          e);
      return CustomSignatureConfigContext.getDefaultInstance();
    }
  }

  private LoadingCache<ContextualKey<CustomSignatureConfigContextKey>, CustomSignatureConfigContext>
      buildCache(CustomSignatureConfigServiceConfig config) {
    return CacheBuilder.newBuilder()
        .maximumSize(config.getCustomSignatureConfigContextCacheMaxSize())
        .refreshAfterWrite(config.getCustomSignatureConfigContextCacheRefreshAfterWriteDuration())
        .recordStats()
        .build(
            CacheLoader.asyncReloading(
                createCacheLoader(),
                Executors.newFixedThreadPool(
                    config.getCustomSignatureConfigContextCacheThreadPoolSize(),
                    new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("custom-signature-config-ctxt-cache-%d")
                        .build())));
  }

  private CacheLoader<ContextualKey<CustomSignatureConfigContextKey>, CustomSignatureConfigContext>
      createCacheLoader() {
    return new CacheLoader<>() {

      @Override
      public CustomSignatureConfigContext load(
          @Nonnull ContextualKey<CustomSignatureConfigContextKey> key) {
        try {
          GetRulesFilter filter = createEventTypeFilter(key.getData().getRequest());
          return loadCustomSignatureConfigContext(
              key.getContext(), key.getData().getRequest(), filter);
        } catch (Exception e) {
          log.error(
              "Error loading from cache for tenant: {}, returning default instance",
              key.getContext().getTenantId(),
              e);
          return CustomSignatureConfigContext.getDefaultInstance();
        }
      }

      @Override
      public ListenableFuture<CustomSignatureConfigContext> reload(
          @Nonnull ContextualKey<CustomSignatureConfigContextKey> cacheKey,
          @Nonnull CustomSignatureConfigContext existingValue) {
        try {
          GetRulesFilter filter = createEventTypeFilter(cacheKey.getData().getRequest());
          return Futures.immediateFuture(
              loadCustomSignatureConfigContext(
                  cacheKey.getContext(), cacheKey.getData().getRequest(), filter));
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

  private void registerCacheMetrics(CustomSignatureConfigServiceConfig config) {
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        CACHE_NAME,
        this.cache,
        Collections.emptyMap(),
        config.getCustomSignatureConfigContextCacheMaxSize());
  }

  private void handleConfigChangeEvent(ConfigChangeEventKey key, ConfigChangeEventValue value) {
    if (!key.getConfigType().equals(CustomSignatureRule.class.getName())) {
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
            cacheKey ->
                cacheKey.getContext().getTenantId().isPresent()
                    && cacheKey.getContext().getTenantId().get().equals(tenantId));
  }
}
