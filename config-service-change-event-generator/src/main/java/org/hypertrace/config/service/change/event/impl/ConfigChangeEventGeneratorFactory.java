package org.hypertrace.config.service.change.event.impl;

import com.typesafe.config.Config;
import java.time.Clock;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class ConfigChangeEventGeneratorFactory {

  static final String GENERIC_CONFIG_SERVICE_PUBLISH_CHANGE_EVENTS =
      "generic.config.service.publish.change.events";

  private static final ConfigChangeEventGeneratorFactory instance =
      new ConfigChangeEventGeneratorFactory();

  private ConfigChangeEventGeneratorFactory() {}

  public static ConfigChangeEventGeneratorFactory getInstance() {
    return instance;
  }

  public ConfigChangeEventGenerator createConfigChangeEventGenerator(
      Config appConfig, Clock clock) {
    if (appConfig.getBoolean(GENERIC_CONFIG_SERVICE_PUBLISH_CHANGE_EVENTS)) {
      return new ConfigChangeEventGeneratorImpl(appConfig, clock);
    } else {
      return new NoopConfigChangeEventGenerator();
    }
  }
}
