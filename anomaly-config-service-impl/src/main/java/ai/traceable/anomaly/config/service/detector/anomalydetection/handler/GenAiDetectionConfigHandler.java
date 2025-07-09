package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.registry.genai.GenAiRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.GenAiAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GenAiDetectionConfigHandler {

  private final Map<String, GenAiAnomalyDetectionConfig> genAiRuleIdToConfigMap;

  GenAiDetectionConfigHandler(GenAiRulesRegistry genAiRulesRegistry) {
    this.genAiRuleIdToConfigMap = genAiRulesRegistry.getGenAiRuleIdToConfigMap();
  }

  /**
   * @param preferredConfig
   * @param fallbackConfig
   * @return List of genAi anomaly detection configs merged using config case as a key
   */
  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    Map<GenAiAnomalyDetectionConfig.ConfigCase, AnomalyDetectionConfig> configCaseMap =
        new HashMap<>();

    getGenAiConfigs(preferredConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              GenAiAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getGenAiAnomalyDetectionConfig().getConfigCase();
              String ruleId = detectionConfig.getGenAiAnomalyDetectionConfig().getAnomalyRuleId();
              if (configCase.equals(GenAiAnomalyDetectionConfig.ConfigCase.CONFIG_NOT_SET)) {
                if (!genAiRuleIdToConfigMap.containsKey(ruleId)) {
                  log.error(
                      "Invalid ruleId \"{}\" and empty configCase for GenAiAnomalyDetectionConfig for configScope {}",
                      ruleId,
                      preferredConfig.getConfigScope());
                  return;
                }
                GenAiAnomalyDetectionConfig genAiAnomalyDetectionConfig =
                    genAiRuleIdToConfigMap.get(ruleId);
                configCase = genAiAnomalyDetectionConfig.getConfigCase();
                detectionConfig =
                    (AnomalyDetectionConfig)
                        mergeConfigs(
                            detectionConfig,
                            AnomalyDetectionConfig.newBuilder()
                                .setGenAiAnomalyDetectionConfig(genAiAnomalyDetectionConfig)
                                .build());
              }
              configCaseMap.put(configCase, detectionConfig);
            });

    getGenAiConfigs(fallbackConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              GenAiAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getGenAiAnomalyDetectionConfig().getConfigCase();
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
    if (detectionConfigsToDelete.stream()
        .anyMatch(
            config ->
                config.hasGenAiAnomalyDetectionConfig()
                    && config
                        .getGenAiAnomalyDetectionConfig()
                        .equals(GenAiAnomalyDetectionConfig.getDefaultInstance()))) {
      anomalyDetectionConfigs.forEach(
          anomalyDetectionConfig -> {
            if (anomalyDetectionConfig.hasGenAiAnomalyDetectionConfig()) {
              deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
            } else {
              filteredConfigs.add(anomalyDetectionConfig);
            }
          });
      return filteredConfigs;
    }
    Set<GenAiAnomalyDetectionConfig.ConfigCase> genAiConfigCase =
        getGenAiConfigs(detectionConfigsToDelete).stream()
            .map(
                detectionConfig -> detectionConfig.getGenAiAnomalyDetectionConfig().getConfigCase())
            .collect(Collectors.toSet());

    for (AnomalyDetectionConfig anomalyDetectionConfig : anomalyDetectionConfigs) {
      if (anomalyDetectionConfig.hasGenAiAnomalyDetectionConfig()
          && genAiConfigCase.contains(
              anomalyDetectionConfig.getGenAiAnomalyDetectionConfig().getConfigCase()))
        deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
      else filteredConfigs.add(anomalyDetectionConfig);
    }
    return filteredConfigs;
  }

  private List<AnomalyDetectionConfig> getGenAiConfigs(
      List<AnomalyDetectionConfig> detectionConfigs) {
    return detectionConfigs.stream()
        .filter(AnomalyDetectionConfig::hasGenAiAnomalyDetectionConfig)
        .collect(Collectors.toList());
  }
}
