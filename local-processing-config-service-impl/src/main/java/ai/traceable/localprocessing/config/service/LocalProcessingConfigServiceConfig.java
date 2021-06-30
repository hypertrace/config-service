package ai.traceable.localprocessing.config.service;

import com.typesafe.config.Config;

public class LocalProcessingConfigServiceConfig {
  private final Config config;

  public LocalProcessingConfigServiceConfig(Config config) {
    this.config = config;
  }

  public Config getConfig() {
    return config;
  }
}
