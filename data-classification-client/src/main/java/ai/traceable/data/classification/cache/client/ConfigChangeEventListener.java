package ai.traceable.data.classification.cache.client;

import ai.traceable.data.classification.cache.config.DataClassificationInfoCachingClientConfig;
import com.typesafe.config.Config;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;

class ConfigChangeEventListener {
  private final Config config;
  private final DataClassificationInfoCachingClientConfig dataClassificationInfoCachingClientConfig;
  private final Consumer<ConfigChangeEventKey, ConfigChangeEventValue> kafkaConsumer;

  public ConfigChangeEventListener(
      Config config,
      DataClassificationInfoCachingClientConfig dataClassificationInfoCachingClientConfig) {
    this.config = config;
    this.dataClassificationInfoCachingClientConfig = dataClassificationInfoCachingClientConfig;
    this.kafkaConsumer =
        new KafkaConsumerBuilder(this.config, this.dataClassificationInfoCachingClientConfig)
            .buildKafkaConsumer();
  }

  public KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> addKafkaEventListener(
      BiConsumer<ConfigChangeEventKey, ConfigChangeEventValue> callback) {
    return new KafkaLiveEventListener.Builder<ConfigChangeEventKey, ConfigChangeEventValue>()
        .registerCallback(callback)
        .build(
            this.dataClassificationInfoCachingClientConfig.getConsumerName(),
            this.config,
            this.kafkaConsumer);
  }
}
