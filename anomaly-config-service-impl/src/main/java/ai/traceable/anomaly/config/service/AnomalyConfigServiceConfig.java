package ai.traceable.anomaly.config.service;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

public class AnomalyConfigServiceConfig {

  private static final String ANOMALY_GLOBAL_CONFIG_PATH = "global";
  private static final String DETECTOR_CONFIG_SERVICE_PATH = "detector.config.service";
  private static final String TRAINER_CONFIG_SERVICE_PATH = "trainer.config.service";
  private static final String AGGREGATOR_CONFIG_SERVICE_PATH = "aggregator.config.service";
  private static final String MODSEC_RULE_VERSION_CONFIG_PATH = "modsecRuleVersion";

  private final Config config;

  public AnomalyConfigServiceConfig(Config config) {
    this.config = config;
  }

  public ModsecRuleVersion getModsecRuleVersion() {
    try {
      return this.config.getEnum(ModsecRuleVersion.class, MODSEC_RULE_VERSION_CONFIG_PATH);
    } catch (ConfigException e) {
      return ModsecRuleVersion.MODSEC_RULE_VERSION_V3;
    }
  }

  public Config getAnomalyGlobalConfig() {
    return this.config.getConfig(ANOMALY_GLOBAL_CONFIG_PATH);
  }

  public Config getDetectorConfigServiceConfig() {
    return this.config.getConfig(DETECTOR_CONFIG_SERVICE_PATH);
  }

  public Config getTrainerConfigServiceConfig() {
    return this.config.getConfig(TRAINER_CONFIG_SERVICE_PATH);
  }

  public Config getAggregatorConfigServiceConfig() {
    return this.config.getConfig(AGGREGATOR_CONFIG_SERVICE_PATH);
  }
}
