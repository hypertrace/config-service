package ai.traceable.anomaly.config.service.trainer;

import ai.traceable.anomaly.config.service.AnomalyConfigServiceConfig;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import com.google.inject.Inject;
import java.util.List;

public class TrainerConfigServiceConfig {
  private static final String API_NAMING_TRAINING_CONFIGS_PATH = "apiNamingTrainingConfigs";
  private static final String METADATA_TRAINING_CONFIGS_PATH = "metadataTrainingConfigs";
  private static final String VULNERABILITY_TRAINING_CONFIGS_PATH = "vulnerabilityTrainingConfigs";
  private final List<TrainingConfig> apiNamingTrainingConfigs;
  private final List<TrainingConfig> metadataTrainingConfigs;
  private final List<TrainingConfig> vulnerabilityTrainingConfigs;

  @Inject
  public TrainerConfigServiceConfig(
      AnomalyConfigServiceConfig config, ConfigConverter configConverter) {
    this.apiNamingTrainingConfigs =
        configConverter.convertToTrainingConfigs(
            config.getTrainerConfigServiceConfig().getConfigList(API_NAMING_TRAINING_CONFIGS_PATH));
    this.metadataTrainingConfigs =
        configConverter.convertToTrainingConfigs(
            config.getTrainerConfigServiceConfig().getConfigList(METADATA_TRAINING_CONFIGS_PATH));
    this.vulnerabilityTrainingConfigs =
        configConverter.convertToTrainingConfigs(
            config
                .getTrainerConfigServiceConfig()
                .getConfigList(VULNERABILITY_TRAINING_CONFIGS_PATH));
  }

  public List<TrainingConfig> getApiNamingTrainingConfigs() {
    return this.apiNamingTrainingConfigs;
  }

  public List<TrainingConfig> getMetadataTrainingConfigs() {
    return this.metadataTrainingConfigs;
  }

  public List<TrainingConfig> getVulnerabilityTrainingConfigs() {
    return this.vulnerabilityTrainingConfigs;
  }
}
