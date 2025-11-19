package ai.traceable.attribute.resolution.client;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import ai.traceable.attribute.resolution.config.AttributeResolutionConfigCachingClientConfig;
import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfig;
import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfigServiceGrpc;
import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsFilter;
import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsRequest;
import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsResponse;
import ai.traceable.attribute.resolution.info.AttributeResolutionConfigInfo;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nonnull;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

public class AttributeResolutionConfigCachingClientImpl implements AttributeResolutionConfigClient {
  private final AttributeResolutionConfigCachingClientConfig clientConfig;
  private final LoadingCache<
          ContextualKey<Optional<GetAttributeResolutionConfigsFilter>>,
          AttributeResolutionConfigInfo>
      attributeResolutionConfigCache;
  private final AttributeResolutionConfigServiceGrpc.AttributeResolutionConfigServiceBlockingStub
      attributeResolutionConfigServiceBlockingStub;

  public AttributeResolutionConfigCachingClientImpl(
      AttributeResolutionConfigServiceGrpc.AttributeResolutionConfigServiceBlockingStub
          attributeResolutionConfigServiceBlockingStub,
      AttributeResolutionConfigCachingClientConfig clientConfig) {
    this.clientConfig = clientConfig;
    this.attributeResolutionConfigServiceBlockingStub =
        attributeResolutionConfigServiceBlockingStub;
    this.attributeResolutionConfigCache = buildCache();
    registerCacheMetrics();
  }

  public AttributeResolutionConfigCachingClientImpl(
      KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener,
      AttributeResolutionConfigServiceGrpc.AttributeResolutionConfigServiceBlockingStub
          attributeResolutionConfigServiceBlockingStub,
      AttributeResolutionConfigCachingClientConfig clientConfig) {
    this(attributeResolutionConfigServiceBlockingStub, clientConfig);
    kafkaLiveEventListener.registerCallback(this::updateCacheBasedOnEvent);
  }

  @Override
  public AttributeResolutionConfigInfo getAttributeResolutionConfig(RequestContext requestContext) {
    return this.attributeResolutionConfigCache.getUnchecked(
        requestContext.buildInternalContextualKey(Optional.empty()));
  }

  @Override
  public AttributeResolutionConfigInfo getAttributeResolutionConfig(
      RequestContext requestContext, @Nonnull GetAttributeResolutionConfigsFilter filter) {
    return this.attributeResolutionConfigCache.getUnchecked(
        requestContext.buildInternalContextualKey(Optional.of(filter)));
  }

  private AttributeResolutionConfigInfo fetchAttributeResolutionConfig(
      ContextualKey<Optional<GetAttributeResolutionConfigsFilter>> contextualKey) {
    GetAttributeResolutionConfigsRequest.Builder requestBuilder =
        GetAttributeResolutionConfigsRequest.newBuilder();
    contextualKey.getData().ifPresent(requestBuilder::setFilter);

    GetAttributeResolutionConfigsResponse response =
        contextualKey.callInContext(
            () ->
                attributeResolutionConfigServiceBlockingStub
                    .withDeadlineAfter(
                        this.clientConfig.getTimeoutDuration().toMillis(), MILLISECONDS)
                    .getAttributeResolutionConfigs(requestBuilder.build()));

    Map<String, AttributeResolutionConfig> idToConfigMap =
        Maps.uniqueIndex(response.getConfigsList(), AttributeResolutionConfig::getId);
    return new AttributeResolutionConfigInfo(idToConfigMap);
  }

  private LoadingCache<
          ContextualKey<Optional<GetAttributeResolutionConfigsFilter>>,
          AttributeResolutionConfigInfo>
      buildCache() {
    return CacheBuilder.newBuilder()
        .maximumSize(this.clientConfig.getMaxSize())
        .refreshAfterWrite(this.clientConfig.getRefreshDuration())
        .expireAfterAccess(this.clientConfig.getExpirationDuration())
        .recordStats()
        .build(
            CacheLoader.asyncReloading(
                CacheLoader.from(this::fetchAttributeResolutionConfig),
                Executors.newFixedThreadPool(
                    this.clientConfig.getMaxThreadPoolSize(), buildThreadFactory())));
  }

  private ThreadFactory buildThreadFactory() {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(this.clientConfig.getCacheThreadFactoryName())
        .build();
  }

  private void registerCacheMetrics() {
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        this.clientConfig.getCacheName(),
        this.attributeResolutionConfigCache,
        Collections.emptyMap(),
        this.clientConfig.getMaxSize());
  }

  private void updateCacheBasedOnEvent(ConfigChangeEventKey key, ConfigChangeEventValue value) {
    if (!AttributeResolutionConfig.class.getName().equals(key.getConfigType())) {
      return;
    }
    switch (value.getEventCase()) {
      case CREATE_EVENT:
      case UPDATE_EVENT:
      case DELETE_EVENT:
        this.attributeResolutionConfigCache.asMap().keySet().stream()
            .filter(
                config ->
                    config
                        .getContext()
                        .getTenantId()
                        .map(tenantId -> tenantId.equals(key.getTenantId()))
                        .orElse(false))
            .forEach(attributeResolutionConfigCache::invalidate);
        break;
      default:
    }
  }
}
