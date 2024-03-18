package ai.traceable.data.classification.cache.client;

import ai.traceable.data.classification.cache.config.DataClassificationInfoCachingClientConfig;
import com.typesafe.config.Config;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;

class KafkaConsumerBuilder {
  private static final String SCHEMA_REGISTRY_URL_PATH = "schema.registry.url";
  private final Consumer<ConfigChangeEventKey, ConfigChangeEventValue> kafkaConsumer;

  public KafkaConsumerBuilder(
      Config config,
      DataClassificationInfoCachingClientConfig dataClassificationInfoCachingClientConfig) {
    Map<String, Object> deserConfig =
        Collections.singletonMap(
            SCHEMA_REGISTRY_URL_PATH, config.getString(SCHEMA_REGISTRY_URL_PATH));
    this.kafkaConsumer =
        org.hypertrace.core.kafka.event.listener.KafkaConsumerUtils.getKafkaConsumer(
            config,
            getConfigChangeEventKeyDeser(deserConfig),
            getConfigChangeEventValueDeser(deserConfig));
  }

  public Consumer<ConfigChangeEventKey, ConfigChangeEventValue> buildKafkaConsumer() {
    return this.kafkaConsumer;
  }

  private Deserializer<ConfigChangeEventKey> getConfigChangeEventKeyDeser(
      Map<String, Object> deserConfig) {
    try (KafkaProtobufSerde<ConfigChangeEventKey> configChangeEventKeyKafkaProtobufSerde =
        new KafkaProtobufSerde<>(ConfigChangeEventKey.class)) {
      configChangeEventKeyKafkaProtobufSerde.configure(deserConfig, true);
      return configChangeEventKeyKafkaProtobufSerde.deserializer();
    }
  }

  private Deserializer<ConfigChangeEventValue> getConfigChangeEventValueDeser(
      Map<String, Object> deserConfig) {
    try (Serde<ConfigChangeEventValue> configChangeEventValueSerde =
        new KafkaProtobufSerde<>(ConfigChangeEventValue.class)) {
      configChangeEventValueSerde.configure(deserConfig, false);
      return configChangeEventValueSerde.deserializer();
    }
  }
}
