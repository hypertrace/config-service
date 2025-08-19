package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import ai.traceable.anomaly.config.service.common.AnomalySubRuleConfigUtils;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GenAiAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;

@UtilityClass
public class AnomalyDetectionConfigUtils {
  public static ScopedAnomalyDetectionConfig populateNewFields(
      ScopedAnomalyDetectionConfig scopedConfig) {
    if (scopedConfig == null) {
      return null;
    }
    ScopedAnomalyDetectionConfig.Builder builder =
        scopedConfig.toBuilder().clearAnomalyDetectionConfigs();
    for (AnomalyDetectionConfig config : scopedConfig.getAnomalyDetectionConfigsList()) {
      AnomalyDetectionConfig processedConfig = updateSubRuleConfigs(config);
      builder.addAnomalyDetectionConfigs(processedConfig);
    }
    return builder.build();
  }

  private static AnomalyDetectionConfig updateSubRuleConfigs(AnomalyDetectionConfig config) {
    AnomalyDetectionConfig.Builder builder = config.toBuilder();
    switch (config.getAnomalyDetectionConfigCase()) {
      case MODSECURITY_ANOMALY_DETECTION_CONFIG:
        if (config.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule()) {
          ModsecurityAnomalyDetectionConfig modsecConfig =
              config.getModsecurityAnomalyDetectionConfig();
          ModsecurityAnomalyRuleConfig ruleConfig = modsecConfig.getModsecAnomalyRule();
          List<AnomalySubRuleConfig> processedSubRules =
              ruleConfig.getSubRuleConfigsList().stream()
                  .map(AnomalySubRuleConfigUtils::populateNewFields)
                  .collect(Collectors.toList());
          ModsecurityAnomalyRuleConfig processedRuleConfig =
              ruleConfig.toBuilder()
                  .clearSubRuleConfigs()
                  .addAllSubRuleConfigs(processedSubRules)
                  .build();
          builder.setModsecurityAnomalyDetectionConfig(
              modsecConfig.toBuilder().setModsecAnomalyRule(processedRuleConfig).build());
        }
        return builder.build();
      case GEN_AI_ANOMALY_DETECTION_CONFIG:
        GenAiAnomalyDetectionConfig genAiConfig = config.getGenAiAnomalyDetectionConfig();
        Map<String, AnomalySubRuleConfig> updatedRules =
            updateSubRules(genAiConfig.getSubRuleConfigs().getSubRuleConfigsMap());
        if (updatedRules.isEmpty()) {
          return config;
        }
        builder.setGenAiAnomalyDetectionConfig(
            genAiConfig.toBuilder()
                .setSubRuleConfigs(
                    genAiConfig.getSubRuleConfigs().toBuilder()
                        .clearSubRuleConfigs()
                        .putAllSubRuleConfigs(updatedRules)
                        .build())
                .build());
        return builder.build();
      case API_DEFINITION_METADATA_ANOMALY_DETECTION_CONFIG:
      case SESSION_DEFINITION_METADATA_ANOMALY_DETECTION_CONFIG:
      case VOLUMETRIC_ANOMALY_DETECTION_CONFIG:
      case ACCOUNT_TAKEOVER_ANOMALY_DETECTION_CONFIG:
      case CUSTOM_RULES_ANOMALY_DETECTION_CONFIG:
      case CREDENTIAL_ANOMALY_DETECTION_CONFIG:
      case BLOCKING_METADATA_ANOMALY_DETECTION_CONFIG:
      case API_STATE_BASED_ANOMALY_DETECTION_CONFIG:
      default:
        return builder.build();
    }
  }

  private static Map<String, AnomalySubRuleConfig> updateSubRules(
      Map<String, AnomalySubRuleConfig> subRules) {
    if (subRules.isEmpty()) {
      return subRules;
    }
    return subRules.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, e -> AnomalySubRuleConfigUtils.populateNewFields(e.getValue())));
  }
}
