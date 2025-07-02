package ai.traceable.genai.system.discovery.client;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import ai.traceable.genai.system.discovery.config.GenAiSystemDiscoveryConfigCachingClientConfig;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryConfigServiceGrpc;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRule;
import ai.traceable.genai.system.discovery.config.service.v1.GetGenAiSystemDiscoveryRulesFilter;
import ai.traceable.genai.system.discovery.config.service.v1.GetGenAiSystemDiscoveryRulesRequest;
import ai.traceable.genai.system.discovery.config.service.v1.GetGenAiSystemDiscoveryRulesResponse;
import ai.traceable.genai.system.discovery.info.GenAiSystemDiscoveryConfig;
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

public class GenAiSystemDiscoveryConfigCachingClientImpl
    implements GenAiSystemDiscoveryConfigClient {
  private final GenAiSystemDiscoveryConfigCachingClientConfig
      genAiSystemDiscoveryConfigCachingClientConfig;
  private final LoadingCache<
          ContextualKey<Optional<GetGenAiSystemDiscoveryRulesFilter>>, GenAiSystemDiscoveryConfig>
      genAiSystemDiscoveryConfigCache;
  private final GenAiSystemDiscoveryConfigServiceGrpc.GenAiSystemDiscoveryConfigServiceBlockingStub
      genAiSystemDiscoveryConfigServiceBlockingStub;

  public GenAiSystemDiscoveryConfigCachingClientImpl(
      GenAiSystemDiscoveryConfigServiceGrpc.GenAiSystemDiscoveryConfigServiceBlockingStub
          genAiSystemDiscoveryConfigServiceBlockingStub,
      GenAiSystemDiscoveryConfigCachingClientConfig genAiSystemDiscoveryConfigCachingClientConfig) {
    this.genAiSystemDiscoveryConfigCachingClientConfig =
        genAiSystemDiscoveryConfigCachingClientConfig;
    this.genAiSystemDiscoveryConfigServiceBlockingStub =
        genAiSystemDiscoveryConfigServiceBlockingStub;
    this.genAiSystemDiscoveryConfigCache = buildCache();
    registerCacheMetrics();
  }

  public GenAiSystemDiscoveryConfigCachingClientImpl(
      KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener,
      GenAiSystemDiscoveryConfigServiceGrpc.GenAiSystemDiscoveryConfigServiceBlockingStub
          genAiSystemDiscoveryConfigServiceBlockingStub,
      GenAiSystemDiscoveryConfigCachingClientConfig genAiSystemDiscoveryConfigCachingClientConfig) {
    this(
        genAiSystemDiscoveryConfigServiceBlockingStub,
        genAiSystemDiscoveryConfigCachingClientConfig);
    kafkaLiveEventListener.registerCallback(this::updateCacheBasedOnEvent);
  }

  @Override
  public GenAiSystemDiscoveryConfig getGenAiSystemDiscoveryConfig(RequestContext requestContext) {

    return this.genAiSystemDiscoveryConfigCache.getUnchecked(
        requestContext.buildInternalContextualKey(Optional.empty()));
  }

  @Override
  public GenAiSystemDiscoveryConfig getGenAiSystemDiscoveryConfig(
      RequestContext requestContext, @Nonnull GetGenAiSystemDiscoveryRulesFilter filter) {
    return this.genAiSystemDiscoveryConfigCache.getUnchecked(
        requestContext.buildInternalContextualKey(Optional.of(filter)));
  }

  private GenAiSystemDiscoveryConfig fetchGenAiSystemDiscoveryConfig(
      ContextualKey<Optional<GetGenAiSystemDiscoveryRulesFilter>> contextualKey) {
    GetGenAiSystemDiscoveryRulesRequest.Builder requestBuilder =
        GetGenAiSystemDiscoveryRulesRequest.newBuilder();
    contextualKey.getData().ifPresent(requestBuilder::setFilter);
    GetGenAiSystemDiscoveryRulesResponse genAiSystemDiscoveryResponse =
        contextualKey.callInContext(
            () ->
                genAiSystemDiscoveryConfigServiceBlockingStub
                    .withDeadlineAfter(
                        this.genAiSystemDiscoveryConfigCachingClientConfig
                            .getTimeoutDuration()
                            .toMillis(),
                        MILLISECONDS)
                    .getGenAiSystemDiscoveryRules(requestBuilder.build()));
    Map<String, GenAiSystemDiscoveryRule> ruleIdToGenAiSystemDiscoveryRule =
        Maps.uniqueIndex(
            genAiSystemDiscoveryResponse.getGenAiSystemDiscoveryRulesList(),
            GenAiSystemDiscoveryRule::getRuleId);
    return new GenAiSystemDiscoveryConfig(ruleIdToGenAiSystemDiscoveryRule);
  }

  private ThreadFactory buildGenAiSystemDiscoveryThreadFactory() {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(
            this.genAiSystemDiscoveryConfigCachingClientConfig
                .getGenAiSystemDiscoveryConfigCacheThreadFactoryName())
        .build();
  }

  private LoadingCache<
          ContextualKey<Optional<GetGenAiSystemDiscoveryRulesFilter>>, GenAiSystemDiscoveryConfig>
      buildCache() {
    return CacheBuilder.newBuilder()
        .maximumSize(this.genAiSystemDiscoveryConfigCachingClientConfig.getMaxSize())
        .refreshAfterWrite(this.genAiSystemDiscoveryConfigCachingClientConfig.getRefreshDuration())
        .expireAfterAccess(
            this.genAiSystemDiscoveryConfigCachingClientConfig.getExpirationDuration())
        .recordStats()
        .build(
            CacheLoader.asyncReloading(
                CacheLoader.from(this::fetchGenAiSystemDiscoveryConfig),
                Executors.newFixedThreadPool(
                    this.genAiSystemDiscoveryConfigCachingClientConfig.getMaxThreadPoolSize(),
                    this.buildGenAiSystemDiscoveryThreadFactory())));
  }

  private void registerCacheMetrics() {
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        this.genAiSystemDiscoveryConfigCachingClientConfig.getGenAiSystemDiscoveryConfigCacheName(),
        this.genAiSystemDiscoveryConfigCache,
        Collections.emptyMap(),
        this.genAiSystemDiscoveryConfigCachingClientConfig.getMaxSize());
  }

  private void updateCacheBasedOnEvent(ConfigChangeEventKey key, ConfigChangeEventValue value) {
    if (!GenAiSystemDiscoveryRule.class.getName().equals(key.getConfigType())) {
      return;
    }
    RequestContext requestContext = RequestContext.forTenantId(key.getTenantId());
    switch (value.getEventCase()) {
      case CREATE_EVENT:
      case UPDATE_EVENT:
      case DELETE_EVENT:
        this.genAiSystemDiscoveryConfigCache.invalidate(
            requestContext.buildInternalContextualKey());
        break;
      default:
    }
  }
}
