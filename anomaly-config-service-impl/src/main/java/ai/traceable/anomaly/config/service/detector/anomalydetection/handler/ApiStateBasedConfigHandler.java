package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiStateBasedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class ApiStateBasedConfigHandler {

  /**
   * @param preferredConfig
   * @param fallbackConfig
   * @return List of state based detection configs merged using config case as a key
   */
  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    EnumMap<ApiStateBasedAnomalyDetectionConfig.ConfigCase, AnomalyDetectionConfig> configMap =
        new EnumMap<>(ApiStateBasedAnomalyDetectionConfig.ConfigCase.class);
    getApiStateBasedConfigs(preferredConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig ->
                configMap.put(
                    detectionConfig.getApiStateBasedAnomalyDetectionConfig().getConfigCase(),
                    detectionConfig));

    getApiStateBasedConfigs(fallbackConfig.getAnomalyDetectionConfigsList())
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

  List<AnomalyDetectionConfig> deleteWholeAnomalyDetectionConfigs(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs,
      List<AnomalyDetectionConfig> detectionConfigsToDelete,
      ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder) {

    List<AnomalyDetectionConfig> filteredConfigs = new ArrayList<>();
    Set<ApiStateBasedAnomalyDetectionConfig.ConfigCase> apiStateBasedConfigCases =
        getApiStateBasedConfigs(detectionConfigsToDelete).stream()
            .map(
                detectionConfig ->
                    detectionConfig.getApiStateBasedAnomalyDetectionConfig().getConfigCase())
            .collect(Collectors.toSet());

    for (AnomalyDetectionConfig anomalyDetectionConfig : anomalyDetectionConfigs) {
      if (anomalyDetectionConfig.hasApiStateBasedAnomalyDetectionConfig()
          && apiStateBasedConfigCases.contains(
              anomalyDetectionConfig.getApiStateBasedAnomalyDetectionConfig().getConfigCase())) {
        deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
      } else {
        filteredConfigs.add(anomalyDetectionConfig);
      }
    }
    return filteredConfigs;
  }

  private List<AnomalyDetectionConfig> getApiStateBasedConfigs(
      List<AnomalyDetectionConfig> detectionConfigs) {
    return detectionConfigs.stream()
        .filter(AnomalyDetectionConfig::hasApiStateBasedAnomalyDetectionConfig)
        .collect(Collectors.toList());
  }
}
