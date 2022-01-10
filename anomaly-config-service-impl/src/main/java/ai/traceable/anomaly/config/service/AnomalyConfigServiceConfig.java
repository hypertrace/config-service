package ai.traceable.anomaly.config.service;

import com.typesafe.config.Config;

public class AnomalyConfigServiceConfig {

  private static final String ANOMALY_GLOBAL_CONFIG_PATH = "global";
  private static final String DETECTOR_CONFIG_SERVICE_PATH = "detector.config.service";

  private final Config config;

  public AnomalyConfigServiceConfig(Config config) {
    this.config = config;
  }

  public Config getAnomalyGlobalConfig() {
    return this.config.getConfig(ANOMALY_GLOBAL_CONFIG_PATH);
  }

  public Config getDetectorConfigServiceConfig() {
    return this.config.getConfig(DETECTOR_CONFIG_SERVICE_PATH);
  }
}
