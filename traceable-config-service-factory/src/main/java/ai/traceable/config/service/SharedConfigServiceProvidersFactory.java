package ai.traceable.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClientConfig;
import com.typesafe.config.Config;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKeyDeserializer;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValueDeserializer;
import org.hypertrace.config.service.change.event.impl.ConfigChangeEventGeneratorFactory;
import org.hypertrace.core.kafka.event.listener.KafkaConsumerUtils;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;
import org.hypertrace.entity.change.event.v1.EntityChangeEventKey;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue;

@Slf4j
public class SharedConfigServiceProvidersFactory {
  private final ConcurrentMap<GrpcServiceContainerEnvironment, SharedConfigServiceProviders>
      providersByEnvironment = new ConcurrentHashMap<>();
  private final Clock clock = Clock.systemUTC();
  static final String SERVICE_NAME = "traceable-config-service";
  private static final String CONFIG_CHANGE_EVENTS_KAFKA_CONFIG_NAME =
      "config.change.events.kafka.client";
  private static final String ENTITY_CHANGE_EVENTS_KAFKA_CONFIG_NAME =
      "entity.change.events.kafka.client";
  private static final String SCHEMA_REGISTRY_URL_PATH = "schema.registry.url";
  private static final String KAFKA_CONSUMER_NAME_CONFIG = "consumer.name";

  SharedConfigServiceProviders getProvidersForEnvironment(
      GrpcServiceContainerEnvironment environment) {
    return this.providersByEnvironment.computeIfAbsent(environment, this::buildProviders);
  }

  private SharedConfigServiceProviders buildProviders(GrpcServiceContainerEnvironment environment) {
    Config config = environment.getConfig(SERVICE_NAME);
    return new SharedConfigServiceProviders(
        ConfigChangeEventGeneratorFactory.getInstance()
            .createConfigChangeEventGenerator(config, clock),
        new FeatureCachingClient(
            FeatureCachingClientConfig.fromConfig(config), environment.getChannelRegistry()),
        environment.getChannelRegistry(),
        config,
        environment.getChannelRegistry().forName(environment.getInProcessChannelName()),
        clock,
        buildConfigKafkaLiveEventListener(config),
        buildEntityKafkaLiveEventListener(config));
  }

  private KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue>
      buildConfigKafkaLiveEventListener(Config config) {
    Config kafkaConfig = config.getConfig(CONFIG_CHANGE_EVENTS_KAFKA_CONFIG_NAME);
    Consumer<ConfigChangeEventKey, ConfigChangeEventValue> configChangeEventKafkaConsumer =
        KafkaConsumerUtils.getKafkaConsumer(
            kafkaConfig,
            new ConfigChangeEventKeyDeserializer(),
            new ConfigChangeEventValueDeserializer());

    KafkaLiveEventListener.Builder<ConfigChangeEventKey, ConfigChangeEventValue> builder =
        new KafkaLiveEventListener.Builder<>();

    return builder.build(
        kafkaConfig.getString(KAFKA_CONSUMER_NAME_CONFIG),
        kafkaConfig,
        configChangeEventKafkaConsumer);
  }

  private KafkaLiveEventListener<EntityChangeEventKey, EntityChangeEventValue>
      buildEntityKafkaLiveEventListener(Config config) {
    Config kafkaConfig = config.getConfig(ENTITY_CHANGE_EVENTS_KAFKA_CONFIG_NAME);
    Map<String, String> deserializerConfig =
        Collections.singletonMap(
            SCHEMA_REGISTRY_URL_PATH, kafkaConfig.getString(SCHEMA_REGISTRY_URL_PATH));
    Consumer<EntityChangeEventKey, EntityChangeEventValue> entityChangeEventKafkaConsumer =
        KafkaConsumerUtils.getKafkaConsumer(
            kafkaConfig,
            getEntityChangeEventKeyDeserializer(deserializerConfig),
            getEntityChangeEventValueDeserializer(deserializerConfig));
    KafkaLiveEventListener.Builder<EntityChangeEventKey, EntityChangeEventValue>
        kafkaLiveEventListenerBuilder = new KafkaLiveEventListener.Builder<>();
    return kafkaLiveEventListenerBuilder.build(
        kafkaConfig.getString(KAFKA_CONSUMER_NAME_CONFIG),
        kafkaConfig,
        entityChangeEventKafkaConsumer);
  }

  private Deserializer<EntityChangeEventKey> getEntityChangeEventKeyDeserializer(
      Map<String, String> deserializerConfig) {
    try (KafkaProtobufSerde<EntityChangeEventKey> entityChangeEventKeyKafkaProtobufSerde =
        new KafkaProtobufSerde<>(EntityChangeEventKey.class)) {
      entityChangeEventKeyKafkaProtobufSerde.configure(deserializerConfig, true);
      return entityChangeEventKeyKafkaProtobufSerde.deserializer();
    }
  }

  private Deserializer<EntityChangeEventValue> getEntityChangeEventValueDeserializer(
      Map<String, String> deserializerConfig) {
    try (Serde<EntityChangeEventValue> entityChangeEventValueKafkaProtobufSerde =
        new KafkaProtobufSerde<>(EntityChangeEventValue.class)) {
      entityChangeEventValueKafkaProtobufSerde.configure(deserializerConfig, false);
      return entityChangeEventValueKafkaProtobufSerde.deserializer();
    }
  }
}
