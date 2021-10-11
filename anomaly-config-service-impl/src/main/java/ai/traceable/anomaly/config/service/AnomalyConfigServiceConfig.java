package ai.traceable.anomaly.config.service;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionTrainerConfig;
import com.typesafe.config.Config;

public class AnomalyConfigServiceConfig {

  private static final String ANOMALY_GLOBAL_CONFIG_PATH = "global";
  private static final String APIDEF_TRAINER_CONFIG_PATH = "apiDefinitionTrainerConfig";

  private final Config config;
  private final ConfigConverter configConverter = new ConfigConverter();

  public AnomalyConfigServiceConfig(Config config) {
    this.config = config;
  }

  public Config getAnomalyGlobalConfig() {
    return this.config.getConfig(ANOMALY_GLOBAL_CONFIG_PATH);
  }

  public ApiDefinitionTrainerConfig getApiDefinitionTrainerConfig() {
    return configConverter.convertApiDefinitionTrainerConfig(
        this.config.getConfig(APIDEF_TRAINER_CONFIG_PATH));
  }
}
