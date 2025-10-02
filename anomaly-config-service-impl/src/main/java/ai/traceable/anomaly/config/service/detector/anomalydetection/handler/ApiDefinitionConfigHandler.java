package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.common.AnomalySubRuleConfigUtils;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ApiDefinitionConfigHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApiDefinitionConfigHandler.class);
  private final Map<String, ApiDefinitionMetadataAnomalyDetectionConfig>
      apiDefMetadataAnomalyDetectionConfigMap;
  private final String UNDER_SCORE = "_";

  ApiDefinitionConfigHandler(ApiDefinitionRegistry apiDefinitionRegistry) {
    this.apiDefMetadataAnomalyDetectionConfigMap =
        apiDefinitionRegistry.getApiDefRuleIdToDetectionConfigMap();
  }

  /**
   * @param preferredConfig preferred config to take precedence during merge
   * @param fallbackConfig fallback config to be used when preferred config does not have a specific
   * @return List of api definition detection configs merged using ruleId as a key, in case ruleId
   *     is not present, the config case is used as a key for merging
   */
  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    EnumMap<ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase, AnomalyDetectionConfig>
        configCaseMap = new EnumMap<>(ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.class);

    getApiDefMetadataConfigs(preferredConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getApiDefinitionMetadataAnomalyDetectionConfig().getConfigCase();
              if (configCase.equals(
                  ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.CONFIG_NOT_SET)) {
                String ruleId =
                    detectionConfig
                        .getApiDefinitionMetadataAnomalyDetectionConfig()
                        .getAnomalyRuleId();
                if (!apiDefMetadataAnomalyDetectionConfigMap.containsKey(ruleId)) {
                  LOGGER.error(
                      "Invalid ruleId \"{}\" and empty configCase for ApiDefinitionMetadataAnomalyDetectionConfig for configScope {}",
                      ruleId,
                      preferredConfig.getConfigScope());
                  return;
                }
                ApiDefinitionMetadataAnomalyDetectionConfig apiDefMetadataAnomalyConfig =
                    apiDefMetadataAnomalyDetectionConfigMap.get(
                        detectionConfig
                            .getApiDefinitionMetadataAnomalyDetectionConfig()
                            .getAnomalyRuleId());
                configCase = apiDefMetadataAnomalyConfig.getConfigCase();
                detectionConfig =
                    (AnomalyDetectionConfig)
                        mergeConfigs(
                            detectionConfig,
                            AnomalyDetectionConfig.newBuilder()
                                .setApiDefinitionMetadataAnomalyDetectionConfig(
                                    apiDefMetadataAnomalyConfig)
                                .build());
              }
              configCaseMap.put(configCase, detectionConfig);
            });

    getApiDefMetadataConfigs(fallbackConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getApiDefinitionMetadataAnomalyDetectionConfig().getConfigCase();
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

  List<AnomalyDetectionConfig> deleteWholeAnomalyDetectionConfigs(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs,
      List<AnomalyDetectionConfig> detectionConfigsToDelete,
      ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder) {
    List<AnomalyDetectionConfig> filteredConfigs = new ArrayList<>();
    if (detectionConfigsToDelete.stream()
        .anyMatch(
            config ->
                config.hasApiDefinitionMetadataAnomalyDetectionConfig()
                    && config
                        .getApiDefinitionMetadataAnomalyDetectionConfig()
                        .equals(
                            ApiDefinitionMetadataAnomalyDetectionConfig.getDefaultInstance()))) {
      anomalyDetectionConfigs.forEach(
          anomalyDetectionConfig -> {
            if (anomalyDetectionConfig.hasApiDefinitionMetadataAnomalyDetectionConfig()) {
              deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
            } else {
              filteredConfigs.add(anomalyDetectionConfig);
            }
          });
      return filteredConfigs;
    }
    Set<ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase> apiDefMetadataConfigCases =
        getApiDefMetadataConfigs(detectionConfigsToDelete).stream()
            .map(
                detectionConfig ->
                    detectionConfig
                        .getApiDefinitionMetadataAnomalyDetectionConfig()
                        .getConfigCase())
            .collect(Collectors.toSet());

    for (AnomalyDetectionConfig anomalyDetectionConfig : anomalyDetectionConfigs) {
      if (anomalyDetectionConfig.hasApiDefinitionMetadataAnomalyDetectionConfig()
          && apiDefMetadataConfigCases.contains(
              anomalyDetectionConfig
                  .getApiDefinitionMetadataAnomalyDetectionConfig()
                  .getConfigCase())) {
        deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
      } else {
        filteredConfigs.add(anomalyDetectionConfig);
      }
    }
    return filteredConfigs;
  }

  private List<AnomalyDetectionConfig> getApiDefMetadataConfigs(
      List<AnomalyDetectionConfig> detectionConfigs) {
    return detectionConfigs.stream()
        .filter(AnomalyDetectionConfig::hasApiDefinitionMetadataAnomalyDetectionConfig)
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
                        .getApiDefinitionMetadataAnomalyDetectionConfig()
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
                        .getApiDefinitionMetadataAnomalyDetectionConfig()
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
              String ruleId =
                  config.getApiDefinitionMetadataAnomalyDetectionConfig().getAnomalyRuleId();
              Map<String, AnomalySubRuleConfig> ruleSpecificSubRules =
                  resultMap.entrySet().stream()
                      .filter(entry -> entry.getKey().startsWith(ruleId + UNDER_SCORE))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
              if (ruleSpecificSubRules.isEmpty()) {
                return config;
              }

              return AnomalyDetectionConfig.newBuilder(config)
                  .setApiDefinitionMetadataAnomalyDetectionConfig(
                      config.getApiDefinitionMetadataAnomalyDetectionConfig().toBuilder()
                          .setSubRuleConfigs(
                              config
                                  .getApiDefinitionMetadataAnomalyDetectionConfig()
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
