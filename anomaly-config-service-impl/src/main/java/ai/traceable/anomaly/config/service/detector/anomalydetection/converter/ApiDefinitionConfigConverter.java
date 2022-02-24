package ai.traceable.anomaly.config.service.detector.anomalydetection.converter;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ApiDefinitionConfigConverter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApiDefinitionConfigConverter.class);
  private final Map<String, ApiDefinitionMetadataAnomalyDetectionConfig>
      apiDefMetadataAnomalyDetectionConfigMap;

  ApiDefinitionConfigConverter(ApiDefinitionRegistry apiDefinitionRegistry) {
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

    preferredConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasApiDefinitionMetadataAnomalyDetectionConfig)
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
                    detectionConfig.toBuilder()
                        .mergeFrom(
                            AnomalyDetectionConfig.newBuilder()
                                .setApiDefinitionMetadataAnomalyDetectionConfig(
                                    apiDefMetadataAnomalyConfig)
                                .build())
                        .build();
              }
              configCaseMap.put(configCase, detectionConfig);
            });

    fallbackConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasApiDefinitionMetadataAnomalyDetectionConfig)
        .forEach(
            detectionConfig -> {
              ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getApiDefinitionMetadataAnomalyDetectionConfig().getConfigCase();
              if (configCaseMap.containsKey(configCase)) {
                configCaseMap.put(
                    configCase,
                    detectionConfig.toBuilder().mergeFrom(configCaseMap.get(configCase)).build());
              } else {
                configCaseMap.put(configCase, detectionConfig);
              }
            });

    List<AnomalyDetectionConfig> resolvedConfigs = new ArrayList<>();
    resolvedConfigs.addAll(configCaseMap.values());

    return resolvedConfigs;
  }
}
