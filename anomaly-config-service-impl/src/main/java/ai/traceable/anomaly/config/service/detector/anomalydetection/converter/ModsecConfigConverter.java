package ai.traceable.anomaly.config.service.detector.anomalydetection.converter;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyCategoryConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class ModsecConfigConverter {

  /**
   * @param preferredConfig
   * @param fallbackConfig
   * @return List of modsec detection configs merged using ruleId as a key, the subRuleConfigs are
   *     merged on the basis of their subRuleId for a particular modsec config
   */
  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    Map<String, AnomalyConfigStatusChange> configStatusMap =
        preferredConfig.getAnomalyDetectionConfigsList().stream()
            .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
            .filter(
                detectionConfig ->
                    detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule())
            .collect(
                Collectors.toMap(
                    detectionConfig ->
                        detectionConfig
                            .getModsecurityAnomalyDetectionConfig()
                            .getModsecAnomalyRule()
                            .getAnomalyRuleId(),
                    AnomalyDetectionConfig::getConfigStatus));

    fallbackConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
        .filter(
            detectionConfig ->
                detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule())
        .forEach(
            anomalyDetectionConfig -> {
              String ruleId =
                  anomalyDetectionConfig
                      .getModsecurityAnomalyDetectionConfig()
                      .getModsecAnomalyRule()
                      .getAnomalyRuleId();
              if (!configStatusMap.containsKey(ruleId)) {
                configStatusMap.put(ruleId, anomalyDetectionConfig.getConfigStatus());
              } else {
                configStatusMap.put(
                    ruleId,
                    anomalyDetectionConfig.getConfigStatus().toBuilder()
                        .mergeFrom(configStatusMap.get(ruleId))
                        .build());
              }
            });

    Map<String, AnomalyCategoryConfig> configCategoryMap =
        preferredConfig.getAnomalyDetectionConfigsList().stream()
            .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
            .filter(
                detectionConfig ->
                    detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule())
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getModsecurityAnomalyDetectionConfig()
                            .getModsecAnomalyRule()
                            .getAnomalyRuleId(),
                    AnomalyDetectionConfig::getCategoryConfig));

    fallbackConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
        .filter(
            detectionConfig ->
                detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule())
        .forEach(
            anomalyDetectionConfig -> {
              String ruleId =
                  anomalyDetectionConfig
                      .getModsecurityAnomalyDetectionConfig()
                      .getModsecAnomalyRule()
                      .getAnomalyRuleId();
              if (!configCategoryMap.containsKey(ruleId)) {
                configCategoryMap.put(ruleId, anomalyDetectionConfig.getCategoryConfig());
              } else {
                configCategoryMap.put(
                    ruleId,
                    anomalyDetectionConfig.getCategoryConfig().toBuilder()
                        .mergeFrom(configCategoryMap.get(ruleId))
                        .build());
              }
            });

    Map<String, Map<String, AnomalySubRuleConfig>> modsecConfigMap =
        mergeSubRuleConfigs(
            getModsecAnomalyRuleConfigs(preferredConfig),
            getModsecAnomalyRuleConfigs(fallbackConfig));

    List<AnomalyDetectionConfig> modsecConfigs = new ArrayList<>();

    for (Map.Entry<String, Map<String, AnomalySubRuleConfig>> entry : modsecConfigMap.entrySet()) {
      String anomalyRuleId = entry.getKey();
      AnomalyDetectionConfig.Builder builder = AnomalyDetectionConfig.newBuilder();
      builder.setCategoryConfig(
          configCategoryMap.getOrDefault(
              anomalyRuleId, AnomalyCategoryConfig.getDefaultInstance()));
      builder.setConfigStatus(
          configStatusMap.getOrDefault(
              anomalyRuleId, AnomalyConfigStatusChange.getDefaultInstance()));

      ModsecurityAnomalyRuleConfig.Builder modsecConfigBuilder =
          ModsecurityAnomalyRuleConfig.newBuilder();
      modsecConfigBuilder.setAnomalyRuleId(entry.getKey());
      for (Map.Entry<String, AnomalySubRuleConfig> subRuleConfigEntry :
          entry.getValue().entrySet()) {
        modsecConfigBuilder.addSubRuleConfigs(subRuleConfigEntry.getValue());
      }

      builder.setModsecurityAnomalyDetectionConfig(
          ModsecurityAnomalyDetectionConfig.newBuilder().setModsecAnomalyRule(modsecConfigBuilder));

      modsecConfigs.add(builder.build());
    }

    AnomalyDetectionConfig modsecurityAllDetectionConfig =
        AnomalyDetectionConfig.getDefaultInstance();

    for (AnomalyDetectionConfig detectionConfig : fallbackConfig.getAnomalyDetectionConfigsList()) {
      if (detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAllDetection()) {
        modsecurityAllDetectionConfig = detectionConfig;
        break;
      }
    }

    for (AnomalyDetectionConfig detectionConfig :
        preferredConfig.getAnomalyDetectionConfigsList()) {
      if (detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAllDetection()) {
        modsecurityAllDetectionConfig =
            modsecurityAllDetectionConfig.toBuilder().mergeFrom(detectionConfig).build();
        break;
      }
    }

    if (!modsecurityAllDetectionConfig.equals(AnomalyDetectionConfig.getDefaultInstance())) {
      modsecConfigs.add(modsecurityAllDetectionConfig);
    }

    return modsecConfigs;
  }

  private Map<String, Map<String, AnomalySubRuleConfig>> mergeSubRuleConfigs(
      List<ModsecurityAnomalyRuleConfig> preferredConfigs,
      List<ModsecurityAnomalyRuleConfig> fallbackConfigs) {
    Map<String, Map<String, AnomalySubRuleConfig>> modsecSubRuleConfigMap =
        preferredConfigs.stream()
            .collect(
                Collectors.toMap(
                    ModsecurityAnomalyRuleConfig::getAnomalyRuleId,
                    this::getModsecSubRuleConfigMap));

    fallbackConfigs.forEach(
        modsecConfig -> {
          String anomalyRuleId = modsecConfig.getAnomalyRuleId();
          if (modsecSubRuleConfigMap.containsKey(anomalyRuleId)) {
            Map<String, AnomalySubRuleConfig> subRuleConfigMap =
                modsecSubRuleConfigMap.get(anomalyRuleId);
            modsecConfig
                .getSubRuleConfigsList()
                .forEach(
                    anomalySubRuleConfig -> {
                      String subRuleId = anomalySubRuleConfig.getSubRuleId();
                      if (subRuleConfigMap.containsKey(subRuleId)) {
                        subRuleConfigMap.put(
                            subRuleId,
                            anomalySubRuleConfig.toBuilder()
                                .mergeFrom(subRuleConfigMap.get(subRuleId))
                                .build());
                      } else {
                        subRuleConfigMap.put(subRuleId, anomalySubRuleConfig);
                      }
                    });

          } else {
            modsecSubRuleConfigMap.put(anomalyRuleId, getModsecSubRuleConfigMap(modsecConfig));
          }
        });

    return modsecSubRuleConfigMap;
  }

  private Map<String, AnomalySubRuleConfig> getModsecSubRuleConfigMap(
      ModsecurityAnomalyRuleConfig modsecurityAnomalyDetectionConfig) {
    return modsecurityAnomalyDetectionConfig.getSubRuleConfigsList().stream()
        .collect(
            Collectors.toMap(
                AnomalySubRuleConfig::getSubRuleId, anomalySubRuleConfig -> anomalySubRuleConfig));
  }

  private List<ModsecurityAnomalyRuleConfig> getModsecAnomalyRuleConfigs(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig) {
    return scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
        .map(AnomalyDetectionConfig::getModsecurityAnomalyDetectionConfig)
        .filter(ModsecurityAnomalyDetectionConfig::hasModsecAnomalyRule)
        .map(ModsecurityAnomalyDetectionConfig::getModsecAnomalyRule)
        .collect(Collectors.toList());
  }
}
