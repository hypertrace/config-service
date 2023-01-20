package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CustomRulesConfigHandler {

  /**
   * @param preferredConfig
   * @param fallbackConfig
   * @return List of custom rules configs merged using config case as a key
   */
  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    EnumMap<CustomRulesAnomalyDetectionConfig.ConfigCase, AnomalyDetectionConfig> configMap =
        new EnumMap<>(CustomRulesAnomalyDetectionConfig.ConfigCase.class);

    getCustomRulesConfigs(preferredConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig ->
                configMap.put(
                    detectionConfig.getCustomRulesAnomalyDetectionConfig().getConfigCase(),
                    detectionConfig));

    getCustomRulesConfigs(fallbackConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              CustomRulesAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getCustomRulesAnomalyDetectionConfig().getConfigCase();
              if (configMap.containsKey(configCase)) {
                AnomalyDetectionConfig mergedDetectionConfig =
                    (AnomalyDetectionConfig)
                        mergeConfigs(detectionConfig, configMap.get(configCase));
                configMap.put(configCase, mergedDetectionConfig);
              } else {
                configMap.put(configCase, detectionConfig);
              }
            });

    return configMap.values().stream().collect(Collectors.toUnmodifiableList());
  }

  List<AnomalyDetectionConfig> deleteWholeAnomalyDetectionConfigs(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs,
      List<AnomalyDetectionConfig> detectionConfigsToDelete,
      ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder) {

    List<AnomalyDetectionConfig> filteredConfigs = new ArrayList<>();
    Set<CustomRulesAnomalyDetectionConfig.ConfigCase> customRulesConfigCase =
        getCustomRulesConfigs(detectionConfigsToDelete).stream()
            .map(
                detectionConfig ->
                    detectionConfig.getCustomRulesAnomalyDetectionConfig().getConfigCase())
            .collect(Collectors.toSet());

    for (AnomalyDetectionConfig anomalyDetectionConfig : anomalyDetectionConfigs) {
      if (anomalyDetectionConfig.hasCustomRulesAnomalyDetectionConfig()
          && customRulesConfigCase.contains(
              anomalyDetectionConfig.getCustomRulesAnomalyDetectionConfig().getConfigCase())) {
        deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
      } else {
        filteredConfigs.add(anomalyDetectionConfig);
      }
    }
    return filteredConfigs;
  }

  private List<AnomalyDetectionConfig> getCustomRulesConfigs(
      List<AnomalyDetectionConfig> detectionConfigs) {
    return detectionConfigs.stream()
        .filter(AnomalyDetectionConfig::hasCustomRulesAnomalyDetectionConfig)
        .collect(Collectors.toList());
  }
}
