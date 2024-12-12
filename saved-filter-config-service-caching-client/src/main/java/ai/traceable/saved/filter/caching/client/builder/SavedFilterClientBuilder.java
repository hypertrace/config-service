package ai.traceable.saved.filter.caching.client.builder;

import ai.traceable.saved.filter.caching.client.SavedFilterCachingClient;
import ai.traceable.saved.filter.caching.client.config.SavedFilterCachingClientConfig;
import ai.traceable.saved.filter.config.service.v1.SavedFilterServiceGrpc.SavedFilterServiceBlockingStub;
import com.google.inject.ImplementedBy;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;

@ImplementedBy(SavedFilterClientBuilderImpl.class)
public interface SavedFilterClientBuilder {

  SavedFilterCachingClient build(
      KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener,
      SavedFilterCachingClientConfig cacheConfig,
      SavedFilterServiceBlockingStub savedFilterServiceBlockingStub);

  SavedFilterCachingClient build(
      KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener,
      SavedFilterCachingClientConfig cacheConfig);

  SavedFilterCachingClient build(
      SavedFilterCachingClientConfig cacheConfig,
      SavedFilterServiceBlockingStub savedFilterServiceBlockingStub);
}
