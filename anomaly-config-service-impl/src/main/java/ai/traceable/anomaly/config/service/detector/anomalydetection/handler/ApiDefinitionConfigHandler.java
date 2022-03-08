package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ApiDefinitionConfigHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApiDefinitionConfigHandler.class);
  private final Map<String, ApiDefinitionMetadataAnomalyDetectionConfig>
      apiDefMetadataAnomalyDetectionConfigMap;

  ApiDefinitionConfigHandler(ApiDefinitionRegistry apiDefinitionRegistry) {
    this.apiDefMetadataAnomalyDetectionConfigMap =
        apiDefinitionRegistry.getApiDefRuleIdToDetectionConfigMap();
  }

  /**
   * @param preferredConfig
   * @param fallbackConfig
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

    List<AnomalyDetectionConfig> resolvedConfigs = new ArrayList<>();
    resolvedConfigs.addAll(configCaseMap.values());

    return resolvedConfigs;
  }

  List<AnomalyDetectionConfig> deleteWholeAnomalyDetectionConfigs(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs,
      List<AnomalyDetectionConfig> detectionConfigsToDelete,
      ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder) {

    List<AnomalyDetectionConfig> filteredConfigs = new ArrayList<>();
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
}
