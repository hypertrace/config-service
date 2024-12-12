package ai.traceable.saved.filter.caching.client.builder;

import static org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider;

import ai.traceable.saved.filter.caching.client.SavedFilterCachingClient;
import ai.traceable.saved.filter.caching.client.SavedFilterCachingClientImpl;
import ai.traceable.saved.filter.caching.client.SavedFilterServiceClient;
import ai.traceable.saved.filter.caching.client.cache.SavedFilterLoadingCache;
import ai.traceable.saved.filter.caching.client.config.SavedFilterCachingClientConfig;
import ai.traceable.saved.filter.caching.client.config.SavedFilterClientConfig;
import ai.traceable.saved.filter.config.service.v1.SavedFilterServiceGrpc;
import ai.traceable.saved.filter.config.service.v1.SavedFilterServiceGrpc.SavedFilterServiceBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;

public class SavedFilterClientBuilderImpl implements SavedFilterClientBuilder {

  @Override
  public SavedFilterCachingClient build(
      KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener,
      SavedFilterCachingClientConfig cacheConfig,
      SavedFilterServiceBlockingStub savedFilterServiceBlockingStub) {
    SavedFilterServiceClient savedFilterServiceClient =
        new SavedFilterServiceClient(
            savedFilterServiceBlockingStub, cacheConfig.getSavedFilterClientConfig());

    SavedFilterLoadingCache savedFilterCache =
        new SavedFilterLoadingCache(
            kafkaLiveEventListener,
            cacheConfig.getSavedFilterCacheConfig(),
            savedFilterServiceClient);

    return new SavedFilterCachingClientImpl(savedFilterCache);
  }

  @Override
  public SavedFilterCachingClient build(
      KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener,
      SavedFilterCachingClientConfig cacheConfig) {

    SavedFilterClientConfig savedFilterClientConfig = cacheConfig.getSavedFilterClientConfig();

    ManagedChannel managedChannel =
        createManagedChannel(savedFilterClientConfig.getHost(), savedFilterClientConfig.getPort());

    SavedFilterServiceBlockingStub vulnerabilityServiceBlockingStub =
        SavedFilterServiceGrpc.newBlockingStub(managedChannel)
            .withCallCredentials(getClientCallCredsProvider().get());

    return build(kafkaLiveEventListener, cacheConfig, vulnerabilityServiceBlockingStub);
  }

  @Override
  public SavedFilterCachingClient build(
      SavedFilterCachingClientConfig cacheConfig,
      SavedFilterServiceBlockingStub savedFilterServiceBlockingStub) {
    SavedFilterServiceClient savedFilterServiceClient =
        new SavedFilterServiceClient(
            savedFilterServiceBlockingStub, cacheConfig.getSavedFilterClientConfig());

    SavedFilterLoadingCache savedFilterCache =
        new SavedFilterLoadingCache(
            cacheConfig.getSavedFilterCacheConfig(), savedFilterServiceClient);

    return new SavedFilterCachingClientImpl(savedFilterCache);
  }

  private ManagedChannel createManagedChannel(String host, int port) {
    return ManagedChannelBuilder.forAddress(host, port).build();
  }
}
