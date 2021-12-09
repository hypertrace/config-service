package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.MetadataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.SessionTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.EnumMap;
import java.util.List;

public class TrainingConfigValidator {
  private final AnomalyConfigValidator anomalyConfigValidator;

  @Inject
  public TrainingConfigValidator(AnomalyConfigValidator anomalyConfigValidator) {
    this.anomalyConfigValidator = anomalyConfigValidator;
  }

  public Status validate(GetScopedTrainingConfigRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "GetScopedTrainingConfigRequest should have a valid config scope.");
    }
    return anomalyConfigValidator.validate(request.getConfigScope(), true);
  }

  public Status validate(UpdateScopedTrainingConfigRequest request) {
    if (!request.getScopedTrainingConfig().hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "UpdateScopedTrainingConfigRequest should have a valid config scope.");
    }
    Status status = validate(request.getScopedTrainingConfig().getTrainingConfigsList());
    if (status == Status.OK) {
      return anomalyConfigValidator.validate(
          request.getScopedTrainingConfig().getConfigScope(), true);
    }
    return status;
  }

  private Status validate(List<TrainingConfig> trainingConfigs) {

    EnumMap<MetadataTrainingConfig.ConfigCase, TrainingConfig> metadataTrainingConfigMap =
        new EnumMap<>(MetadataTrainingConfig.ConfigCase.class);

    EnumMap<VulnerabilityTrainingConfig.ConfigCase, TrainingConfig> vulnerabilityTrainingConfigMap =
        new EnumMap<>(VulnerabilityTrainingConfig.ConfigCase.class);

    EnumMap<SessionTrainingConfig.ConfigCase, TrainingConfig> sessionTrainingConfigMap =
        new EnumMap<>(SessionTrainingConfig.ConfigCase.class);

    EnumMap<ApiNamingTrainingConfig.ConfigCase, TrainingConfig> apiNamingTrainingConfigMap =
        new EnumMap<>(ApiNamingTrainingConfig.ConfigCase.class);
    for (TrainingConfig trainingConfig : trainingConfigs) {

      switch (trainingConfig.getTrainingConfigCase()) {
        case METADATA_TRAINING_CONFIG:
          MetadataTrainingConfig.ConfigCase metadataTrainingConfigCase =
              trainingConfig.getMetadataTrainingConfig().getConfigCase();
          if (metadataTrainingConfigMap.containsKey(metadataTrainingConfigCase)) {
            return Status.INVALID_ARGUMENT.withDescription(
                "UpdateScopedTrainingConfigRequest should have only one training config for metadataTrainingConfigType: "
                    + metadataTrainingConfigCase);
          } else {
            metadataTrainingConfigMap.put(metadataTrainingConfigCase, trainingConfig);
          }
          break;
        case VULNERABILITY_TRAINING_CONFIG:
          VulnerabilityTrainingConfig.ConfigCase vulnerabilityTrainingConfigCase =
              trainingConfig.getVulnerabilityTrainingConfig().getConfigCase();
          if (vulnerabilityTrainingConfigMap.containsKey(vulnerabilityTrainingConfigCase)) {
            return Status.INVALID_ARGUMENT.withDescription(
                "UpdateScopedTrainingConfigRequest should have only one training config for vulnerabilityTrainingConfigType: "
                    + vulnerabilityTrainingConfigCase);
          } else {
            vulnerabilityTrainingConfigMap.put(vulnerabilityTrainingConfigCase, trainingConfig);
          }
          break;
        case SESSION_TRAINING_CONFIG:
          SessionTrainingConfig.ConfigCase sessionTrainingConfigCase =
              trainingConfig.getSessionTrainingConfig().getConfigCase();
          if (sessionTrainingConfigMap.containsKey(sessionTrainingConfigCase)) {
            return Status.INVALID_ARGUMENT.withDescription(
                "UpdateScopedTrainingConfigRequest should have only one training config for sessionTrainingConfigType: "
                    + sessionTrainingConfigCase);
          } else {
            sessionTrainingConfigMap.put(sessionTrainingConfigCase, trainingConfig);
          }
          break;
        case API_NAMING_TRAINING_CONFIG:
          ApiNamingTrainingConfig.ConfigCase apiNamingTrainingConfigCase =
              trainingConfig.getApiNamingTrainingConfig().getConfigCase();
          if (apiNamingTrainingConfigMap.containsKey(apiNamingTrainingConfigCase)) {
            return Status.INVALID_ARGUMENT.withDescription(
                "UpdateScopedTrainingConfigRequest should have only one training config for apiNamingTrainingConfigType: "
                    + apiNamingTrainingConfigCase);
          } else {
            apiNamingTrainingConfigMap.put(apiNamingTrainingConfigCase, trainingConfig);
          }
          break;
        default:
          break;
      }
    }
    return Status.OK;
  }
}
