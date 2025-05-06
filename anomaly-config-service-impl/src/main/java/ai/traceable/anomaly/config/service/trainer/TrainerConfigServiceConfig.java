package ai.traceable.anomaly.config.service.trainer;

import ai.traceable.anomaly.config.service.AnomalyConfigServiceConfig;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingActionConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import com.google.inject.Inject;
import java.util.List;

public class TrainerConfigServiceConfig {
  private static final String API_NAMING_TRAINING_CONFIGS_PATH = "apiNamingTrainingConfigs";
  private static final String METADATA_TRAINING_CONFIGS_PATH = "metadataTrainingConfigs";
  private static final String VULNERABILITY_TRAINING_CONFIGS_PATH = "vulnerabilityTrainingConfigs";
  private static final String VOLUMETRIC_TRAINING_CONFIGS_PATH = "volumetricTrainingConfigs";
  private static final String DOMAIN_DISCOVERY_CONFIGS_PATH = "domainDiscoveryConfigs";
  private static final String API_DISCOVERY_CONFIGS_PATH = "apiDiscoveryConfigs";
  private static final String DEFAULT_TRAINING_ACTIONS_PATH = "defaultTrainingActions";

  private final List<TrainingConfig> apiNamingTrainingConfigs;
  private final List<TrainingConfig> metadataTrainingConfigs;
  private final List<TrainingConfig> vulnerabilityTrainingConfigs;
  private final List<TrainingConfig> volumetricTrainingConfigs;
  private final List<TrainingConfig> domainDiscoveryConfigs;
  private final List<TrainingConfig> apiDiscoveryConfigs;
  private final List<TrainingActionConfig> defaultTrainingActionConfigs;

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
    this.volumetricTrainingConfigs =
        configConverter.convertToTrainingConfigs(
            config.getTrainerConfigServiceConfig().getConfigList(VOLUMETRIC_TRAINING_CONFIGS_PATH));
    this.domainDiscoveryConfigs =
        configConverter.convertToTrainingConfigs(
            config.getTrainerConfigServiceConfig().getConfigList(DOMAIN_DISCOVERY_CONFIGS_PATH));
    this.apiDiscoveryConfigs =
        configConverter.convertToTrainingConfigs(
            config.getTrainerConfigServiceConfig().getConfigList(API_DISCOVERY_CONFIGS_PATH));
    this.defaultTrainingActionConfigs =
        configConverter.convertToTrainingActionConfigs(
            config.getTrainerConfigServiceConfig().getConfigList(DEFAULT_TRAINING_ACTIONS_PATH));
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

  public List<TrainingConfig> getVolumetricTrainingConfigs() {
    return this.volumetricTrainingConfigs;
  }

  public List<TrainingConfig> getDomainDiscoveryConfigs() {
    return this.domainDiscoveryConfigs;
  }

  public List<TrainingConfig> getApiDiscoveryConfigs() {
    return this.apiDiscoveryConfigs;
  }

  public List<TrainingActionConfig> getDefaultTrainingActionConfigs() {
    return this.defaultTrainingActionConfigs;
  }
}
