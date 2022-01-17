package ai.traceable.anomaly.config.service.detector;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DetectorConfigServiceConfig {
  private static final String MODSEC_DETECTION_CONFIGS_PATH = "modsecDetectionConfigs";
  private static final String API_DEFINITION_DETECTION_CONFIGS_PATH =
      "apiDefinitionDetectionConfigs";
  private final List<AnomalyDetectionConfig> modsecDetectionConfigs;
  private final List<AnomalyDetectionConfig> apiDefinitionDetectionConfigs;
  private final ConfigConverter configConverter = new ConfigConverter();

  public DetectorConfigServiceConfig(Config config, ApiDefinitionRegistry apiDefinitionRegistry) {
    this.modsecDetectionConfigs =
        configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(MODSEC_DETECTION_CONFIGS_PATH));
    this.apiDefinitionDetectionConfigs =
        loadDefaultApiDefinitionDetectionConfigs(
            config,
            apiDefinitionRegistry.getApiDefRuleIdToDetectionConfigMap().values().stream()
                .map(
                    detectionConfig ->
                        AnomalyDetectionConfig.newBuilder()
                            .setApiDefinitionMetadataAnomalyDetectionConfig(detectionConfig)
                            .build())
                .collect(Collectors.toList()));
  }

  public List<AnomalyDetectionConfig> getDefaultModsecDetectionConfigs() {
    return modsecDetectionConfigs;
  }

  public List<AnomalyDetectionConfig> getDefaultApiDefinitionDetectionConfigs() {
    return apiDefinitionDetectionConfigs;
  }

  private List<AnomalyDetectionConfig> loadDefaultApiDefinitionDetectionConfigs(
      Config config, List<AnomalyDetectionConfig> apiDefinitionDetectionRegistryConfigs) {
    List<AnomalyDetectionConfig> detectionConfigs =
        configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(API_DEFINITION_DETECTION_CONFIGS_PATH));

    Map<String, AnomalyDetectionConfig> configMap =
        detectionConfigs.stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getApiDefinitionMetadataAnomalyDetectionConfig()
                            .getAnomalyRuleId(),
                    anomalyDetectionConfig -> anomalyDetectionConfig));

    return apiDefinitionDetectionRegistryConfigs.stream()
        .map(
            detectionConfig -> {
              String ruleId =
                  detectionConfig
                      .getApiDefinitionMetadataAnomalyDetectionConfig()
                      .getAnomalyRuleId();
              if (configMap.containsKey(ruleId)) {
                return detectionConfig.toBuilder().mergeFrom(configMap.get(ruleId)).build();
              }
              return detectionConfig;
            })
        .collect(Collectors.toList());
  }
}
