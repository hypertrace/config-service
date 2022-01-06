package ai.traceable.anomaly.config.service.detector.anomalydetection;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AnomalyDetectionConfigValidator {
  private final AnomalyConfigValidator anomalyConfigValidator;

  @Inject
  public AnomalyDetectionConfigValidator(AnomalyConfigValidator anomalyConfigValidator) {
    this.anomalyConfigValidator = anomalyConfigValidator;
  }

  public Status validate(GetScopedAnomalyDetectionConfigRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "GetScopedAnomalyDetectionConfigRequest should have a valid config scope.");
    }
    return anomalyConfigValidator.validate(request.getConfigScope(), true);
  }

  public Status validate(UpdateScopedAnomalyDetectionConfigRequest request) {
    if (!request.getScopedAnomalyDetectionConfig().hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "UpdateScopedAnomalyDetectionConfigRequest should have a valid config scope.");
    }
    Status status =
        validate(request.getScopedAnomalyDetectionConfig().getAnomalyDetectionConfigsList());
    if (status == Status.OK) {
      return anomalyConfigValidator.validate(
          request.getScopedAnomalyDetectionConfig().getConfigScope(), true);
    }
    return status;
  }

  private Status validate(List<AnomalyDetectionConfig> detectionConfigs) {
    List<ModsecurityAnomalyDetectionConfig> modsecConfigs =
        detectionConfigs.stream()
            .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
            .map(AnomalyDetectionConfig::getModsecurityAnomalyDetectionConfig)
            .collect(Collectors.toList());

    return validateModsecConfigs(modsecConfigs);
  }

  private Status validateModsecConfigs(List<ModsecurityAnomalyDetectionConfig> modsecConfigs) {
    Map<String, Map<String, AnomalySubRuleConfig>> configMap = new HashMap<>();

    for (ModsecurityAnomalyDetectionConfig detectionConfig : modsecConfigs) {
      String ruleId = detectionConfig.getAnomalyRuleId();
      if (configMap.containsKey(ruleId)) {
        return Status.INVALID_ARGUMENT.withDescription(
            "UpdateScopedAnomalyDetectionConfigRequest should have only one modsec config with ruleId: "
                + ruleId);
      } else {
        Map<String, AnomalySubRuleConfig> subRuleConfigMap = new HashMap<>();
        for (AnomalySubRuleConfig subRuleConfig : detectionConfig.getSubRuleConfigsList()) {
          String subRuleId = subRuleConfig.getSubRuleId();
          if (subRuleConfigMap.containsKey(subRuleId)) {
            return Status.INVALID_ARGUMENT.withDescription(
                "UpdateScopedAnomalyDetectionConfigRequest should have only one subRule config with subRuleId: "
                    + subRuleId
                    + " in modsec config with ruleId: "
                    + ruleId);
          } else {
            subRuleConfigMap.put(subRuleId, subRuleConfig);
          }
        }
        configMap.put(ruleId, subRuleConfigMap);
      }
    }
    return Status.OK;
  }
}
