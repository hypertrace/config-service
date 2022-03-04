package ai.traceable.anomaly.config.service.detector.anomalydetection.converter;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiStateBasedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;

class ApiStateBasedConfigConverter {

  /**
   * @param preferredConfig
   * @param fallbackConfig
   * @return List of state based detection configs merged using config case as a key
   */
  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    EnumMap<ApiStateBasedAnomalyDetectionConfig.ConfigCase, AnomalyDetectionConfig> configMap =
        new EnumMap<>(ApiStateBasedAnomalyDetectionConfig.ConfigCase.class);
    preferredConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasApiStateBasedAnomalyDetectionConfig)
        .forEach(
            detectionConfig ->
                configMap.put(
                    detectionConfig.getApiStateBasedAnomalyDetectionConfig().getConfigCase(),
                    detectionConfig));

    fallbackConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasApiStateBasedAnomalyDetectionConfig)
        .forEach(
            detectionConfig -> {
              ApiStateBasedAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getApiStateBasedAnomalyDetectionConfig().getConfigCase();
              if (configMap.containsKey(configCase)) {
                AnomalyDetectionConfig mergedDetectionConfig =
                    (AnomalyDetectionConfig)
                        mergeConfigs(detectionConfig, configMap.get(configCase));
                configMap.put(configCase, mergedDetectionConfig);
              } else {
                configMap.put(configCase, detectionConfig);
              }
            });

    return configMap.values().stream().collect(Collectors.toList());
  }
}
