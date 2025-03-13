package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyCategoryConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class ModsecConfigHandler {

  /**
   * @param preferredConfig
   * @param fallbackConfig
   * @return List of modsec detection configs merged using ruleId as a key, the subRuleConfigs are
   *     merged on the basis of their subRuleId for a particular modsec config
   */
  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    return merge(
        preferredConfig.getAnomalyDetectionConfigsList(),
        fallbackConfig.getAnomalyDetectionConfigsList());
  }

  public List<AnomalyDetectionConfig> merge(
      List<AnomalyDetectionConfig> preferredAnomalyDetectionConfigList,
      List<AnomalyDetectionConfig> fallbackAnomalyDetectionConfigList) {
    List<AnomalyDetectionConfig> preferredModsecAnomalyRuleConfigs =
        getModsecRuleConfigs(preferredAnomalyDetectionConfigList);
    List<AnomalyDetectionConfig> fallbackModsecAnomalyRuleConfigs =
        getModsecRuleConfigs(fallbackAnomalyDetectionConfigList);
    Map<String, AnomalyConfigStatusChange> configStatusMap =
        preferredModsecAnomalyRuleConfigs.stream()
            .collect(
                Collectors.toMap(
                    detectionConfig ->
                        detectionConfig
                            .getModsecurityAnomalyDetectionConfig()
                            .getModsecAnomalyRule()
                            .getAnomalyRuleId(),
                    AnomalyDetectionConfig::getConfigStatus));

    fallbackModsecAnomalyRuleConfigs.forEach(
        anomalyDetectionConfig -> {
          String ruleId =
              anomalyDetectionConfig
                  .getModsecurityAnomalyDetectionConfig()
                  .getModsecAnomalyRule()
                  .getAnomalyRuleId();
          if (!configStatusMap.containsKey(ruleId)) {
            configStatusMap.put(ruleId, anomalyDetectionConfig.getConfigStatus());
          } else {
            AnomalyConfigStatusChange mergedConfigStatusChange =
                (AnomalyConfigStatusChange)
                    mergeConfigs(
                        anomalyDetectionConfig.getConfigStatus(), configStatusMap.get(ruleId));
            configStatusMap.put(ruleId, mergedConfigStatusChange);
          }
        });

    Map<String, AnomalyCategoryConfig> configCategoryMap =
        preferredModsecAnomalyRuleConfigs.stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getModsecurityAnomalyDetectionConfig()
                            .getModsecAnomalyRule()
                            .getAnomalyRuleId(),
                    AnomalyDetectionConfig::getCategoryConfig));

    fallbackModsecAnomalyRuleConfigs.forEach(
        anomalyDetectionConfig -> {
          String ruleId =
              anomalyDetectionConfig
                  .getModsecurityAnomalyDetectionConfig()
                  .getModsecAnomalyRule()
                  .getAnomalyRuleId();
          if (!configCategoryMap.containsKey(ruleId)) {
            configCategoryMap.put(ruleId, anomalyDetectionConfig.getCategoryConfig());
          } else {
            AnomalyCategoryConfig mergedCategoryConfig =
                (AnomalyCategoryConfig)
                    mergeConfigs(
                        anomalyDetectionConfig.getCategoryConfig(), configCategoryMap.get(ruleId));
            configCategoryMap.put(ruleId, mergedCategoryConfig);
          }
        });

    Map<String, Map<String, AnomalySubRuleConfig>> modsecConfigMap =
        mergeSubRuleConfigs(
            getModsecAnomalyRuleConfigs(preferredModsecAnomalyRuleConfigs),
            getModsecAnomalyRuleConfigs(fallbackModsecAnomalyRuleConfigs));

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
        (AnomalyDetectionConfig)
            mergeConfigs(
                getModsecAllDetectionConfig(fallbackAnomalyDetectionConfigList),
                getModsecAllDetectionConfig(preferredAnomalyDetectionConfigList));

    if (!modsecurityAllDetectionConfig.equals(AnomalyDetectionConfig.getDefaultInstance())) {
      modsecConfigs.add(modsecurityAllDetectionConfig);
    }

    return modsecConfigs;
  }

  List<AnomalyDetectionConfig> deleteWholeAnomalyDetectionConfigs(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs,
      List<AnomalyDetectionConfig> detectionConfigsToDelete,
      ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder) {
    List<AnomalyDetectionConfig> filteredConfigs = new ArrayList<>();
    if (detectionConfigsToDelete.stream()
        .anyMatch(
            config ->
                config.hasModsecurityAnomalyDetectionConfig()
                    && config
                        .getModsecurityAnomalyDetectionConfig()
                        .equals(ModsecurityAnomalyDetectionConfig.getDefaultInstance()))) {
      anomalyDetectionConfigs.forEach(
          anomalyDetectionConfig -> {
            if (anomalyDetectionConfig.hasModsecurityAnomalyDetectionConfig()) {
              deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
            } else {
              filteredConfigs.add(anomalyDetectionConfig);
            }
          });
      return filteredConfigs;
    }

    Set<String> modsecRuleIds =
        getModsecRuleConfigs(detectionConfigsToDelete).stream()
            .map(
                detectionConfig ->
                    detectionConfig
                        .getModsecurityAnomalyDetectionConfig()
                        .getModsecAnomalyRule()
                        .getAnomalyRuleId())
            .collect(Collectors.toSet());

    AnomalyDetectionConfig modsecAllDetectionConfig =
        getModsecAllDetectionConfig(detectionConfigsToDelete);

    for (AnomalyDetectionConfig anomalyDetectionConfig : anomalyDetectionConfigs) {
      if (anomalyDetectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule()
          && modsecRuleIds.contains(
              anomalyDetectionConfig
                  .getModsecurityAnomalyDetectionConfig()
                  .getModsecAnomalyRule()
                  .getAnomalyRuleId())) {
        deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
      } else if (anomalyDetectionConfig
              .getModsecurityAnomalyDetectionConfig()
              .hasModsecAllDetection()
          && !modsecAllDetectionConfig.equals(AnomalyDetectionConfig.getDefaultInstance())) {
        deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
      } else {
        filteredConfigs.add(anomalyDetectionConfig);
      }
    }
    return filteredConfigs;
  }

  private List<AnomalyDetectionConfig> getModsecRuleConfigs(
      List<AnomalyDetectionConfig> detectionConfigs) {
    return detectionConfigs.stream()
        .filter(
            detectionConfig ->
                detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule())
        .collect(Collectors.toList());
  }

  private AnomalyDetectionConfig getModsecAllDetectionConfig(
      List<AnomalyDetectionConfig> detectionConfigs) {
    return detectionConfigs.stream()
        .filter(
            detectionConfig ->
                detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAllDetection())
        .findFirst()
        .orElse(AnomalyDetectionConfig.getDefaultInstance());
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
                        AnomalySubRuleConfig mergedAnomalySubRuleConfig =
                            (AnomalySubRuleConfig)
                                mergeConfigs(anomalySubRuleConfig, subRuleConfigMap.get(subRuleId));
                        subRuleConfigMap.put(subRuleId, mergedAnomalySubRuleConfig);
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
      List<AnomalyDetectionConfig> anomalyDetectionConfigList) {
    return anomalyDetectionConfigList.stream()
        .map(AnomalyDetectionConfig::getModsecurityAnomalyDetectionConfig)
        .filter(ModsecurityAnomalyDetectionConfig::hasModsecAnomalyRule)
        .map(ModsecurityAnomalyDetectionConfig::getModsecAnomalyRule)
        .collect(Collectors.toList());
  }
}
