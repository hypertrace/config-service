package org.hypertrace.config.service.change.event.impl;

import static org.hypertrace.config.service.change.event.impl.ConfigChangeEventGeneratorFactory.GENERIC_CONFIG_SERVICE_PUBLISH_CHANGE_EVENTS;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Clock;
import java.util.Map;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class ConfigChangeEventGeneratorFactoryTest {

  @Test
  void createNoopConfigChangeEventGenerator() {
    Config config =
        ConfigFactory.parseMap(Map.of(GENERIC_CONFIG_SERVICE_PUBLISH_CHANGE_EVENTS, "false"));
    ConfigChangeEventGenerator configChangeEventGenerator =
        ConfigChangeEventGeneratorFactory.getInstance()
            .createConfigChangeEventGenerator(config, Clock.systemUTC());
    assertTrue(configChangeEventGenerator instanceof NoopConfigChangeEventGenerator);
  }

  @Test
  void createConfigChangeEventGeneratorImpl() {
    Config config = getEventStoreConfig();
    ConfigChangeEventGenerator configChangeEventGenerator =
        ConfigChangeEventGeneratorFactory.getInstance()
            .createConfigChangeEventGenerator(config, Clock.systemUTC());
    assertTrue(configChangeEventGenerator instanceof ConfigChangeEventGeneratorImpl);
  }

  private Config getEventStoreConfig() {
    return ConfigFactory.parseMap(
        Map.of(
            GENERIC_CONFIG_SERVICE_PUBLISH_CHANGE_EVENTS,
            "true",
            "event.store",
            Map.of(
                "type",
                "kafka",
                "bootstrap.servers",
                "localhost:9092",
                "config.change.events.producer",
                Map.of(
                    "topic.name",
                    "config-change-events",
                    "bootstrap.servers",
                    "localhost:9092",
                    "key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer",
                    "value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer",
                    "schema.registry.url",
                    "http://localhost:8081"))));
  }
}
