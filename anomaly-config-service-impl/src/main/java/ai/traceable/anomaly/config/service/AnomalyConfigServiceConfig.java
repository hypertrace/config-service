package ai.traceable.anomaly.config.service;

import com.typesafe.config.Config;

public class AnomalyConfigServiceConfig {

  private static final String ANOMALY_GLOBAL_CONFIG_PATH = "global";

  private final Config config;

  public AnomalyConfigServiceConfig(Config config) {
    this.config = config;
  }

  public Config getAnomalyGlobalConfig() {
    return this.config.getConfig(ANOMALY_GLOBAL_CONFIG_PATH);
  }
}
