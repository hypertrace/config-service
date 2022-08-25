package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.DeleteAnomalyConfigOption;
import ai.traceable.anomaly.config.service.v1.trainer.DeleteScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetUnresolvedScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.LocalTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.MetadataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.SensitiveDataTrainingConfig;
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

  public Status validate(GetUnresolvedScopedTrainingConfigRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "GetUnresolvedScopedTrainingConfigRequest should have a valid config scope.");
    }
    if (!request.hasFilter()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "GetUnresolvedScopedTrainingConfigRequest should have a valid config filter.");
    }
    return anomalyConfigValidator.validate(request.getConfigScope(), true);
  }

  public Status validate(DeleteScopedTrainingConfigRequest request) {
    if (!request.getScopedTrainingConfig().hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "DeleteScopedTrainingConfigRequest should have a valid config scope.");
    }
    if (request
        .getDeleteAnomalyConfigOption()
        .equals(DeleteAnomalyConfigOption.DELETE_ANOMALY_CONFIG_OPTION_UNSPECIFIED)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "DeleteScopedTrainingConfigRequest should have a valid delete option.");
    }
    Status status =
        anomalyConfigValidator.validate(request.getScopedTrainingConfig().getConfigScope(), true);
    if (!status.isOk()) {
      return status;
    }
    return validate(request.getScopedTrainingConfig().getTrainingConfigsList());
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

    EnumMap<SensitiveDataTrainingConfig.ConfigCase, TrainingConfig> sensitiveDataTrainingConfigMap =
        new EnumMap<>(SensitiveDataTrainingConfig.ConfigCase.class);

    EnumMap<LocalTrainingConfig.ConfigCase, TrainingConfig> localTrainingConfigMap =
        new EnumMap<>(LocalTrainingConfig.ConfigCase.class);

    for (TrainingConfig trainingConfig : trainingConfigs) {

      switch (trainingConfig.getTrainingConfigCase()) {
        case METADATA_TRAINING_CONFIG:
          MetadataTrainingConfig.ConfigCase metadataTrainingConfigCase =
              trainingConfig.getMetadataTrainingConfig().getConfigCase();
          if (metadataTrainingConfigMap.containsKey(metadataTrainingConfigCase)) {
            return Status.INVALID_ARGUMENT.withDescription(
                String.format(
                    "UpdateScopedTrainingConfigRequest should have only one training config for metadataTrainingConfigType: %s",
                    metadataTrainingConfigCase));
          } else {
            metadataTrainingConfigMap.put(metadataTrainingConfigCase, trainingConfig);
          }
          break;
        case VULNERABILITY_TRAINING_CONFIG:
          VulnerabilityTrainingConfig.ConfigCase vulnerabilityTrainingConfigCase =
              trainingConfig.getVulnerabilityTrainingConfig().getConfigCase();
          if (vulnerabilityTrainingConfigMap.containsKey(vulnerabilityTrainingConfigCase)) {
            return Status.INVALID_ARGUMENT.withDescription(
                String.format(
                    "UpdateScopedTrainingConfigRequest should have only one training config for vulnerabilityTrainingConfigType: %s",
                    vulnerabilityTrainingConfigCase));
          } else {
            vulnerabilityTrainingConfigMap.put(vulnerabilityTrainingConfigCase, trainingConfig);
          }
          break;
        case SESSION_TRAINING_CONFIG:
          SessionTrainingConfig.ConfigCase sessionTrainingConfigCase =
              trainingConfig.getSessionTrainingConfig().getConfigCase();
          if (sessionTrainingConfigMap.containsKey(sessionTrainingConfigCase)) {
            return Status.INVALID_ARGUMENT.withDescription(
                String.format(
                    "UpdateScopedTrainingConfigRequest should have only one training config for sessionTrainingConfigType: %s",
                    sessionTrainingConfigCase));
          } else {
            sessionTrainingConfigMap.put(sessionTrainingConfigCase, trainingConfig);
          }
          break;
        case API_NAMING_TRAINING_CONFIG:
          ApiNamingTrainingConfig.ConfigCase apiNamingTrainingConfigCase =
              trainingConfig.getApiNamingTrainingConfig().getConfigCase();
          if (apiNamingTrainingConfigMap.containsKey(apiNamingTrainingConfigCase)) {
            return Status.INVALID_ARGUMENT.withDescription(
                String.format(
                    "UpdateScopedTrainingConfigRequest should have only one training config for apiNamingTrainingConfigType: %s",
                    apiNamingTrainingConfigCase));
          } else {
            apiNamingTrainingConfigMap.put(apiNamingTrainingConfigCase, trainingConfig);
          }
          break;
        case SENSITIVE_DATA_TRAINING_CONFIG:
          SensitiveDataTrainingConfig.ConfigCase sensitiveDataTrainingConfigCase =
              trainingConfig.getSensitiveDataTrainingConfig().getConfigCase();
          if (sensitiveDataTrainingConfigMap.containsKey(sensitiveDataTrainingConfigCase)) {
            return Status.INVALID_ARGUMENT.withDescription(
                String.format(
                    "UpdateScopedTrainingConfigRequest should have only one training config for sensitiveDataTrainingConfigType: %s",
                    sensitiveDataTrainingConfigCase));
          } else {
            sensitiveDataTrainingConfigMap.put(sensitiveDataTrainingConfigCase, trainingConfig);
          }
          break;

        case LOCAL_TRAINING_CONFIG:
          LocalTrainingConfig.ConfigCase localTrainingConfigCase =
              trainingConfig.getLocalTrainingConfig().getConfigCase();
          if (localTrainingConfigMap.containsKey(localTrainingConfigCase)) {
            return Status.INVALID_ARGUMENT.withDescription(
                "UpdateScopedTrainingConfigRequest should have only one training config for localTrainingConfig: "
                    + localTrainingConfigCase);
          } else {
            localTrainingConfigMap.put(localTrainingConfigCase, trainingConfig);
          }
          break;

        default:
          break;
      }
    }
    return Status.OK;
  }
}
