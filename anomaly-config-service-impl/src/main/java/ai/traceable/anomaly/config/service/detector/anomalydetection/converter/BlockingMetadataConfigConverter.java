package ai.traceable.anomaly.config.service.detector.anomalydetection.converter;

import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.BlockingMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;

class BlockingMetadataConfigConverter {

  /**
   * @param preferredConfig
   * @param fallbackConfig
   * @return List of blocking detection configs merged using config case as a key
   */
  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    EnumMap<BlockingMetadataAnomalyDetectionConfig.ConfigCase, AnomalyDetectionConfig> configMap =
        new EnumMap<>(BlockingMetadataAnomalyDetectionConfig.ConfigCase.class);

    preferredConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasBlockingMetadataAnomalyDetectionConfig)
        .forEach(
            detectionConfig ->
                configMap.put(
                    detectionConfig.getBlockingMetadataAnomalyDetectionConfig().getConfigCase(),
                    detectionConfig));

    fallbackConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasBlockingMetadataAnomalyDetectionConfig)
        .forEach(
            detectionConfig -> {
              BlockingMetadataAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getBlockingMetadataAnomalyDetectionConfig().getConfigCase();
              if (configMap.containsKey(configCase)) {
                configMap.put(
                    configCase,
                    detectionConfig.toBuilder().mergeFrom(configMap.get(configCase)).build());
              } else {
                configMap.put(configCase, detectionConfig);
              }
            });

    return configMap.values().stream().collect(Collectors.toList());
  }
}
