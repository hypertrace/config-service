package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.VolumetricAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class VolumetricDetectionConfigHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(VolumetricDetectionConfigHandler.class);
  private final Map<String, VolumetricAnomalyDetectionConfig> volumetricRuleIdToConfigMap;

  VolumetricDetectionConfigHandler(VolumetricRulesRegistry volumetricRulesRegistry) {
    this.volumetricRuleIdToConfigMap = volumetricRulesRegistry.getVolumetricRuleIdToConfigMap();
  }

  /**
   * @param preferredConfig
   * @param fallbackConfig
   * @return List of volumetric anomaly detection configs merged using config case as a key
   */
  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    EnumMap<VolumetricAnomalyDetectionConfig.ConfigCase, AnomalyDetectionConfig> configCaseMap =
        new EnumMap<>(VolumetricAnomalyDetectionConfig.ConfigCase.class);

    getVolumetricConfigs(preferredConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              VolumetricAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getVolumetricAnomalyDetectionConfig().getConfigCase();
              if (configCase.equals(VolumetricAnomalyDetectionConfig.ConfigCase.CONFIG_NOT_SET)) {
                String ruleId =
                    detectionConfig.getVolumetricAnomalyDetectionConfig().getAnomalyRuleId();
                if (!volumetricRuleIdToConfigMap.containsKey(ruleId)) {
                  LOGGER.error(
                      "Invalid ruleId \"{}\" and empty configCase for VolumetricAnomalyDetectionConfig for configScope {}",
                      ruleId,
                      preferredConfig.getConfigScope());
                  return;
                }
                VolumetricAnomalyDetectionConfig volumetricAnomalyConfig =
                    volumetricRuleIdToConfigMap.get(ruleId);
                configCase = volumetricAnomalyConfig.getConfigCase();
                detectionConfig =
                    (AnomalyDetectionConfig)
                        mergeConfigs(
                            detectionConfig,
                            AnomalyDetectionConfig.newBuilder()
                                .setVolumetricAnomalyDetectionConfig(volumetricAnomalyConfig)
                                .build());
              }
              configCaseMap.put(configCase, detectionConfig);
            });

    getVolumetricConfigs(fallbackConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              VolumetricAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getVolumetricAnomalyDetectionConfig().getConfigCase();
              if (configCaseMap.containsKey(configCase)) {
                AnomalyDetectionConfig mergedDetectionConfig =
                    (AnomalyDetectionConfig)
                        mergeConfigs(detectionConfig, configCaseMap.get(configCase));
                configCaseMap.put(configCase, mergedDetectionConfig);
              } else {
                configCaseMap.put(configCase, detectionConfig);
              }
            });

    return new ArrayList<>(configCaseMap.values());
  }

  List<AnomalyDetectionConfig> deleteWholeAnomalyDetectionConfig(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs,
      List<AnomalyDetectionConfig> detectionConfigsToDelete,
      ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder) {

    List<AnomalyDetectionConfig> filteredConfigs = new ArrayList<>();
    Set<VolumetricAnomalyDetectionConfig.ConfigCase> volumetricConfigCase =
        getVolumetricConfigs(detectionConfigsToDelete).stream()
            .map(
                detectionConfig ->
                    detectionConfig.getVolumetricAnomalyDetectionConfig().getConfigCase())
            .collect(Collectors.toSet());

    for (AnomalyDetectionConfig anomalyDetectionConfig : anomalyDetectionConfigs) {
      if (anomalyDetectionConfig.hasVolumetricAnomalyDetectionConfig()
          && volumetricConfigCase.contains(
              anomalyDetectionConfig.getVolumetricAnomalyDetectionConfig().getConfigCase()))
        deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
      else filteredConfigs.add(anomalyDetectionConfig);
    }
    return filteredConfigs;
  }

  private List<AnomalyDetectionConfig> getVolumetricConfigs(
      List<AnomalyDetectionConfig> detectionConfigs) {
    return detectionConfigs.stream()
        .filter(AnomalyDetectionConfig::hasVolumetricAnomalyDetectionConfig)
        .collect(Collectors.toList());
  }
}
