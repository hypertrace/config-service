package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.common.AnomalySubRuleConfigUtils;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.VolumetricAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class VolumetricDetectionConfigHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(VolumetricDetectionConfigHandler.class);
  private final Map<String, VolumetricAnomalyDetectionConfig> volumetricRuleIdToConfigMap;
  private final String UNDER_SCORE = "_";

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
    Map<String, AnomalyDetectionConfig> ruleIdToConfigMap = new HashMap<>();

    getVolumetricConfigs(preferredConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              VolumetricAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getVolumetricAnomalyDetectionConfig().getConfigCase();
              String ruleId =
                  detectionConfig.getVolumetricAnomalyDetectionConfig().getAnomalyRuleId();
              if (configCase.equals(VolumetricAnomalyDetectionConfig.ConfigCase.CONFIG_NOT_SET)) {
                if (!volumetricRuleIdToConfigMap.containsKey(ruleId)) {
                  LOGGER.error(
                      "Invalid ruleId \"{}\" and empty configCase for VolumetricAnomalyDetectionConfig for configScope {}",
                      ruleId,
                      preferredConfig.getConfigScope());
                  return;
                }
                VolumetricAnomalyDetectionConfig volumetricAnomalyConfig =
                    volumetricRuleIdToConfigMap.get(ruleId);
                detectionConfig =
                    (AnomalyDetectionConfig)
                        mergeConfigs(
                            detectionConfig,
                            AnomalyDetectionConfig.newBuilder()
                                .setVolumetricAnomalyDetectionConfig(volumetricAnomalyConfig)
                                .build());
              }
              ruleIdToConfigMap.put(ruleId, detectionConfig);
            });

    getVolumetricConfigs(fallbackConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              String ruleId =
                  detectionConfig.getVolumetricAnomalyDetectionConfig().getAnomalyRuleId();
              if (ruleIdToConfigMap.containsKey(ruleId)) {
                AnomalyDetectionConfig mergedDetectionConfig =
                    (AnomalyDetectionConfig)
                        mergeConfigs(detectionConfig, ruleIdToConfigMap.get(ruleId));
                ruleIdToConfigMap.put(ruleId, mergedDetectionConfig);
              } else {
                ruleIdToConfigMap.put(ruleId, detectionConfig);
              }
            });

    return new ArrayList<>(
        populateNewFields(
            ruleIdToConfigMap.values(), preferredConfig.getAnomalyDetectionConfigsList()));
  }

  List<AnomalyDetectionConfig> deleteWholeAnomalyDetectionConfig(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs,
      List<AnomalyDetectionConfig> detectionConfigsToDelete,
      ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder) {
    List<AnomalyDetectionConfig> filteredConfigs = new ArrayList<>();
    if (detectionConfigsToDelete.stream()
        .anyMatch(
            config ->
                config.hasVolumetricAnomalyDetectionConfig()
                    && config
                        .getVolumetricAnomalyDetectionConfig()
                        .equals(VolumetricAnomalyDetectionConfig.getDefaultInstance()))) {
      anomalyDetectionConfigs.forEach(
          anomalyDetectionConfig -> {
            if (anomalyDetectionConfig.hasVolumetricAnomalyDetectionConfig()) {
              deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
            } else {
              filteredConfigs.add(anomalyDetectionConfig);
            }
          });
      return filteredConfigs;
    }
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
                        .getVolumetricAnomalyDetectionConfig()
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
                        .getVolumetricAnomalyDetectionConfig()
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
              String ruleId = config.getVolumetricAnomalyDetectionConfig().getAnomalyRuleId();
              Map<String, AnomalySubRuleConfig> ruleSpecificSubRules =
                  resultMap.entrySet().stream()
                      .filter(entry -> entry.getKey().startsWith(ruleId + UNDER_SCORE))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
              if (ruleSpecificSubRules.isEmpty()) {
                return config;
              }

              return AnomalyDetectionConfig.newBuilder(config)
                  .setVolumetricAnomalyDetectionConfig(
                      config.getVolumetricAnomalyDetectionConfig().toBuilder()
                          .setSubRuleConfigs(
                              config
                                  .getVolumetricAnomalyDetectionConfig()
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
