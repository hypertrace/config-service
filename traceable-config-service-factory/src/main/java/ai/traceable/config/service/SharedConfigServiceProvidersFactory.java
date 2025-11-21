package ai.traceable.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClientConfig;
import com.typesafe.config.Config;
import java.time.Clock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKeyDeserializer;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValueDeserializer;
import org.hypertrace.config.service.change.event.impl.ConfigChangeEventGeneratorFactory;
import org.hypertrace.core.kafka.event.listener.KafkaConsumerUtils;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;

@Slf4j
public class SharedConfigServiceProvidersFactory {
  private final ConcurrentMap<GrpcServiceContainerEnvironment, SharedConfigServiceProviders>
      providersByEnvironment = new ConcurrentHashMap<>();
  private final Clock clock = Clock.systemUTC();
  static final String SERVICE_NAME = "traceable-config-service";
  private static final String CONFIG_CHANGE_EVENTS_KAFKA_CONFIG_NAME =
      "config.change.events.kafka.client";
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
        buildKafkaLiveEventListener(config));
  }

  private KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue>
      buildKafkaLiveEventListener(Config config) {
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
}
