package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.common.AnomalySubRuleConfigUtils;
import ai.traceable.anomaly.config.service.registry.genai.GenAiRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GenAiAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.*;
import java.util.function.Function;
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

    return new ArrayList<>(
        populateNewFields(
            configCaseMap.values(), preferredConfig.getAnomalyDetectionConfigsList()));
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

  private List<AnomalyDetectionConfig> populateNewFields(
      Collection<AnomalyDetectionConfig> mergedConfigs,
      List<AnomalyDetectionConfig> preferredConfigs) {
    if (mergedConfigs.isEmpty() || preferredConfigs.isEmpty()) {
      return new ArrayList<>(mergedConfigs);
    }

    Map<String, AnomalySubRuleConfig> preferredSubRuleConfigs =
        preferredConfigs.stream()
            .flatMap(
                config ->
                    config
                        .getGenAiAnomalyDetectionConfig()
                        .getSubRuleConfigs()
                        .getSubRuleConfigsMap()
                        .values()
                        .stream())
            .collect(Collectors.toMap(AnomalySubRuleConfig::getSubRuleId, Function.identity()));

    Map<String, AnomalySubRuleConfig> mergedSubRuleConfigs =
        mergedConfigs.stream()
            .flatMap(
                config ->
                    config
                        .getGenAiAnomalyDetectionConfig()
                        .getSubRuleConfigs()
                        .getSubRuleConfigsMap()
                        .values()
                        .stream())
            .collect(Collectors.toMap(AnomalySubRuleConfig::getSubRuleId, Function.identity()));
    Map<String, AnomalySubRuleConfig> resultMap = new HashMap<>();
    for (AnomalySubRuleConfig mergedConfig : mergedSubRuleConfigs.values()) {
      if (preferredSubRuleConfigs.get(mergedConfig.getSubRuleId()) != null) {
        AnomalySubRuleConfig preferredConfig =
            preferredSubRuleConfigs.get(mergedConfig.getSubRuleId());
        resultMap.put(
            mergedConfig.getSubRuleId(),
            AnomalySubRuleConfigUtils.handleMergedConfigChange(mergedConfig, preferredConfig));
      } else {
        resultMap.put(
            mergedConfig.getSubRuleId(), AnomalySubRuleConfigUtils.populateNewFields(mergedConfig));
      }
    }
    if (resultMap.isEmpty()) {
      return new ArrayList<>(mergedConfigs);
    }

    return mergedConfigs.stream()
        .map(
            config -> {
              String ruleId = config.getGenAiAnomalyDetectionConfig().getAnomalyRuleId();
              Map<String, AnomalySubRuleConfig> ruleSpecificSubRules =
                  resultMap.entrySet().stream()
                      .filter(entry -> entry.getKey().startsWith(ruleId + "_"))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
              if (ruleSpecificSubRules.isEmpty()) {
                return config;
              }

              return AnomalyDetectionConfig.newBuilder(config)
                  .setGenAiAnomalyDetectionConfig(
                      config.getGenAiAnomalyDetectionConfig().toBuilder()
                          .setSubRuleConfigs(
                              config
                                  .getGenAiAnomalyDetectionConfig()
                                  .getSubRuleConfigs()
                                  .toBuilder()
                                  .clearSubRuleConfigs()
                                  .putAllSubRuleConfigs(ruleSpecificSubRules))
                          .build())
                  .build();
            })
        .collect(Collectors.toList());
  }
}
