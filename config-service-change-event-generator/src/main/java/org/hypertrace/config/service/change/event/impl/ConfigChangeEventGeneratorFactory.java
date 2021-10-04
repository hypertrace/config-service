package org.hypertrace.config.service.change.event.impl;

import com.typesafe.config.Config;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class ConfigChangeEventGeneratorFactory {

  static final String PUBLISH_CHANGE_EVENTS = "publish.change.events";

  private static final ConfigChangeEventGeneratorFactory instance =
      new ConfigChangeEventGeneratorFactory();

  private ConfigChangeEventGeneratorFactory() {}

  public static ConfigChangeEventGeneratorFactory getInstance() {
    return instance;
  }

  public ConfigChangeEventGenerator createConfigChangeEventGenerator(Config appConfig) {
    if (appConfig.getBoolean(PUBLISH_CHANGE_EVENTS)) {
      return new ConfigChangeEventGeneratorImpl(appConfig);
    } else {
      return new NoopConfigChangeEventGenerator();
    }
  }
}
