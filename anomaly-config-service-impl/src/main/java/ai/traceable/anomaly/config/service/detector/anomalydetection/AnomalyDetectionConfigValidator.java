package ai.traceable.anomaly.config.service.detector.anomalydetection;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiStateBasedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AnomalyDetectionConfigValidator {
  private final AnomalyConfigValidator anomalyConfigValidator;
  private final Map<String, ApiDefinitionMetadataAnomalyDetectionConfig> apiDefRuleIdToConfigMap;

  @Inject
  public AnomalyDetectionConfigValidator(
      AnomalyConfigValidator anomalyConfigValidator, ApiDefinitionRegistry apiDefinitionRegistry) {
    this.anomalyConfigValidator = anomalyConfigValidator;
    this.apiDefRuleIdToConfigMap = apiDefinitionRegistry.getApiDefRuleIdToDetectionConfigMap();
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

    Status status = validateModsecConfigs(modsecConfigs);

    if (!status.isOk()) {
      return status;
    }

    List<ApiDefinitionMetadataAnomalyDetectionConfig> apiDefinitionDetectionConfigs =
        detectionConfigs.stream()
            .filter(AnomalyDetectionConfig::hasApiDefinitionMetadataAnomalyDetectionConfig)
            .map(AnomalyDetectionConfig::getApiDefinitionMetadataAnomalyDetectionConfig)
            .collect(Collectors.toList());

    status = validateApiDefinitionConfigs(apiDefinitionDetectionConfigs);

    if (!status.isOk()) {
      return status;
    }

    List<ApiStateBasedAnomalyDetectionConfig> stateBasedDetectionConfigs =
        detectionConfigs.stream()
            .filter(AnomalyDetectionConfig::hasApiStateBasedAnomalyDetectionConfig)
            .map(AnomalyDetectionConfig::getApiStateBasedAnomalyDetectionConfig)
            .collect(Collectors.toList());

    status = validateStateBasedDetectionConfigs(stateBasedDetectionConfigs);
    return status;
  }

  /**
   * @param apiDefinitionDetectionConfigs
   * @return Status.INVALID_ARGUMENT in case both ruleId and config case are not present, ruleId is
   *     not present in apiDef registry, mismatch of ruleId and config case or presence of configs
   *     with same ruleId or configCase. Status.OK in all other cases.
   */
  private Status validateApiDefinitionConfigs(
      List<ApiDefinitionMetadataAnomalyDetectionConfig> apiDefinitionDetectionConfigs) {

    Map<String, ApiDefinitionMetadataAnomalyDetectionConfig> ruleIdMap = new HashMap<>();
    EnumMap<
            ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase,
            ApiDefinitionMetadataAnomalyDetectionConfig>
        configCaseMap = new EnumMap<>(ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.class);

    for (ApiDefinitionMetadataAnomalyDetectionConfig detectionConfig :
        apiDefinitionDetectionConfigs) {
      String ruleId = detectionConfig.getAnomalyRuleId();
      ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase =
          detectionConfig.getConfigCase();

      if (ruleId.isEmpty()) {
        if (configCase.equals(
            ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.CONFIG_NOT_SET)) {
          return Status.INVALID_ARGUMENT.withDescription("Invalid api definition detection config");
        }
      } else {
        if (!apiDefRuleIdToConfigMap.containsKey(ruleId)) {
          return Status.INVALID_ARGUMENT.withDescription(
              "Invalid api definition detection config ruleId: " + ruleId);
        }
        if (!configCase.equals(
                ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.CONFIG_NOT_SET)
            && !configCase.equals(apiDefRuleIdToConfigMap.get(ruleId).getConfigCase())) {
          return Status.INVALID_ARGUMENT.withDescription(
              "Invalid api definition detection config type for ruleId: " + ruleId);
        }
      }

      if (ruleIdMap.containsKey(ruleId)) {
        return Status.INVALID_ARGUMENT.withDescription(
            "UpdateScopedAnomalyDetectionConfigRequest should have only one api definition detection config for ruleId: "
                + ruleId);
      }

      if (configCaseMap.containsKey(configCase)) {
        return Status.INVALID_ARGUMENT.withDescription(
            "UpdateScopedAnomalyDetectionConfigRequest should have only one api definition detection config for configCase: "
                + configCase);
      }

      if (ruleId.isEmpty()) {
        configCaseMap.put(configCase, detectionConfig);
      } else {
        ruleIdMap.put(ruleId, detectionConfig);
      }
    }

    return Status.OK;
  }

  /**
   * @param modsecConfigs
   * @return Status.INVALID_ARGUMENT in case of presence of configs with same ruleId or presence of
   *     subRuleConfigs with same subRuleId for a particular modsec config. Status.OK in all other
   *     cases.
   */
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

  /**
   * @param stateBasedDetectionConfigs
   * @return Status.INVALID_ARGUMENT in case of presence of configs with same config case. Status.OK
   *     in all other cases.
   */
  private Status validateStateBasedDetectionConfigs(
      List<ApiStateBasedAnomalyDetectionConfig> stateBasedDetectionConfigs) {
    try {
      stateBasedDetectionConfigs.stream()
          .collect(
              Collectors.toMap(
                  ApiStateBasedAnomalyDetectionConfig::getConfigCase,
                  detectionConfig -> detectionConfig));
    } catch (IllegalStateException e) {
      return Status.INVALID_ARGUMENT.withDescription(e.getMessage());
    }
    return Status.OK;
  }
}
