package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.BlockingMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class BlockingMetadataConfigHandler {

  /**
   * @param preferredConfig
   * @param fallbackConfig
   * @return List of blocking detection configs merged using config case as a key
   */
  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    EnumMap<BlockingMetadataAnomalyDetectionConfig.ConfigCase, AnomalyDetectionConfig> configMap =
        new EnumMap<>(BlockingMetadataAnomalyDetectionConfig.ConfigCase.class);

    getBlockingMetadataConfigs(preferredConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig ->
                configMap.put(
                    detectionConfig.getBlockingMetadataAnomalyDetectionConfig().getConfigCase(),
                    detectionConfig));

    getBlockingMetadataConfigs(fallbackConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              BlockingMetadataAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getBlockingMetadataAnomalyDetectionConfig().getConfigCase();
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
    if (detectionConfigsToDelete.stream()
        .anyMatch(
            config ->
                config.hasBlockingMetadataAnomalyDetectionConfig()
                    && config
                        .getBlockingMetadataAnomalyDetectionConfig()
                        .equals(BlockingMetadataAnomalyDetectionConfig.getDefaultInstance()))) {
      anomalyDetectionConfigs.forEach(
          anomalyDetectionConfig -> {
            if (anomalyDetectionConfig.hasBlockingMetadataAnomalyDetectionConfig()) {
              deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
            } else {
              filteredConfigs.add(anomalyDetectionConfig);
            }
          });
      return filteredConfigs;
    }
    Set<BlockingMetadataAnomalyDetectionConfig.ConfigCase> blockingMetadataConfigCases =
        getBlockingMetadataConfigs(detectionConfigsToDelete).stream()
            .map(
                detectionConfig ->
                    detectionConfig.getBlockingMetadataAnomalyDetectionConfig().getConfigCase())
            .collect(Collectors.toSet());

    for (AnomalyDetectionConfig anomalyDetectionConfig : anomalyDetectionConfigs) {
      if (anomalyDetectionConfig.hasBlockingMetadataAnomalyDetectionConfig()
          && blockingMetadataConfigCases.contains(
              anomalyDetectionConfig.getBlockingMetadataAnomalyDetectionConfig().getConfigCase())) {
        deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
      } else {
        filteredConfigs.add(anomalyDetectionConfig);
      }
    }
    return filteredConfigs;
  }

  private List<AnomalyDetectionConfig> getBlockingMetadataConfigs(
      List<AnomalyDetectionConfig> detectionConfigs) {
    return detectionConfigs.stream()
        .filter(AnomalyDetectionConfig::hasBlockingMetadataAnomalyDetectionConfig)
        .collect(Collectors.toList());
  }
}
