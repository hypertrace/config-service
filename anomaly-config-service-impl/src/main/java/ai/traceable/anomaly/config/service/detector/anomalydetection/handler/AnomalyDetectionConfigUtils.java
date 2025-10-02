package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import ai.traceable.anomaly.config.service.common.AnomalySubRuleConfigUtils;
import ai.traceable.anomaly.config.service.v1.detector.AccountTakeoverAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.GenAiAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.VolumetricAnomalyDetectionConfig;
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
        ApiDefinitionMetadataAnomalyDetectionConfig apiDefConfig =
            config.getApiDefinitionMetadataAnomalyDetectionConfig();
        Map<String, AnomalySubRuleConfig> updatedApiDefRules =
            updateSubRules(apiDefConfig.getSubRuleConfigs().getSubRuleConfigsMap());
        if (updatedApiDefRules.isEmpty()) {
          return config;
        }
        builder.setApiDefinitionMetadataAnomalyDetectionConfig(
            apiDefConfig.toBuilder()
                .setSubRuleConfigs(
                    apiDefConfig.getSubRuleConfigs().toBuilder()
                        .clearSubRuleConfigs()
                        .putAllSubRuleConfigs(updatedApiDefRules)
                        .build())
                .build());
        return builder.build();
      case SESSION_DEFINITION_METADATA_ANOMALY_DETECTION_CONFIG:
        SessionDefinitionMetadataAnomalyDetectionConfig sessionDefConfig =
            config.getSessionDefinitionMetadataAnomalyDetectionConfig();
        Map<String, AnomalySubRuleConfig> updatedSessionDefRules =
            updateSubRules(sessionDefConfig.getSubRuleConfigs().getSubRuleConfigsMap());
        if (updatedSessionDefRules.isEmpty()) {
          return config;
        }
        builder.setSessionDefinitionMetadataAnomalyDetectionConfig(
            sessionDefConfig.toBuilder()
                .setSubRuleConfigs(
                    sessionDefConfig.getSubRuleConfigs().toBuilder()
                        .clearSubRuleConfigs()
                        .putAllSubRuleConfigs(updatedSessionDefRules)
                        .build())
                .build());
        return builder.build();
      case VOLUMETRIC_ANOMALY_DETECTION_CONFIG:
        VolumetricAnomalyDetectionConfig volumetricConfig =
            config.getVolumetricAnomalyDetectionConfig();
        Map<String, AnomalySubRuleConfig> updatedVolumetricRules =
            updateSubRules(volumetricConfig.getSubRuleConfigs().getSubRuleConfigsMap());
        if (updatedVolumetricRules.isEmpty()) {
          return config;
        }
        builder.setVolumetricAnomalyDetectionConfig(
            volumetricConfig.toBuilder()
                .setSubRuleConfigs(
                    volumetricConfig.getSubRuleConfigs().toBuilder()
                        .clearSubRuleConfigs()
                        .putAllSubRuleConfigs(updatedVolumetricRules)
                        .build())
                .build());
        return builder.build();
      case ACCOUNT_TAKEOVER_ANOMALY_DETECTION_CONFIG:
        AccountTakeoverAnomalyDetectionConfig accountTakeOverConfig =
            config.getAccountTakeoverAnomalyDetectionConfig();
        Map<String, AnomalySubRuleConfig> updatedAccountTakeOverRules =
            updateSubRules(accountTakeOverConfig.getSubRuleConfigs().getSubRuleConfigsMap());
        if (updatedAccountTakeOverRules.isEmpty()) {
          return config;
        }
        builder.setAccountTakeoverAnomalyDetectionConfig(
            accountTakeOverConfig.toBuilder()
                .setSubRuleConfigs(
                    accountTakeOverConfig.getSubRuleConfigs().toBuilder()
                        .clearSubRuleConfigs()
                        .putAllSubRuleConfigs(updatedAccountTakeOverRules)
                        .build())
                .build());
        return builder.build();
      case CREDENTIAL_ANOMALY_DETECTION_CONFIG:
      case CUSTOM_RULES_ANOMALY_DETECTION_CONFIG:
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
