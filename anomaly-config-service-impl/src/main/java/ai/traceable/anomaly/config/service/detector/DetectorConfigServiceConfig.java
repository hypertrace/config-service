package ai.traceable.anomaly.config.service.detector;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DetectorConfigServiceConfig {
  private static final String MODSEC_DETECTION_CONFIGS_PATH = "modsecDetectionConfigs";
  private static final String API_DEFINITION_DETECTION_CONFIGS_PATH =
      "apiDefinitionDetectionConfigs";
  private static final String SESSION_DEFINITION_DETECTION_CONFIGS_PATH =
      "sessionDefinitionDetectionConfigs";
  private static final String CUSTOM_RULES_DETECTION_CONFIGS_PATH = "customRulesDetectionConfigs";
  private static final String VOLUMETRIC_DETECTION_CONFIGS_PATH = "volumetricDetectionConfigs";

  private final List<AnomalyDetectionConfig> modsecDetectionConfigs;
  private final List<AnomalyDetectionConfig> apiDefinitionDetectionConfigs;
  private final List<AnomalyDetectionConfig> sessionDefinitionDetectionConfigs;
  private final List<AnomalyDetectionConfig> customRulesDetectionConfigs;
  private final List<AnomalyDetectionConfig> volumetricDetectionConfigs;

  private final ConfigConverter configConverter = new ConfigConverter();

  public DetectorConfigServiceConfig(
      Config config,
      ApiDefinitionRegistry apiDefinitionRegistry,
      SessionRulesRegistry sessionDefinitionRegistry,
      VolumetricRulesRegistry volumetricRulesRegistry) {
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
    this.sessionDefinitionDetectionConfigs =
        loadDefaultSessionDefinitionDetectionConfigs(
            config,
            sessionDefinitionRegistry.getSessionDefRuleIdToDetectionConfigMap().values().stream()
                .map(
                    detectionConfig ->
                        AnomalyDetectionConfig.newBuilder()
                            .setSessionDefinitionMetadataAnomalyDetectionConfig(detectionConfig)
                            .build())
                .collect(Collectors.toList()));
    this.customRulesDetectionConfigs =
        configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(CUSTOM_RULES_DETECTION_CONFIGS_PATH));

    this.volumetricDetectionConfigs =
        loadDefaultVolumetricDetectionConfigs(
            config,
            volumetricRulesRegistry.getVolumetricRuleIdToConfigMap().values().stream()
                .map(
                    detectionConfig ->
                        AnomalyDetectionConfig.newBuilder()
                            .setVolumetricAnomalyDetectionConfig(detectionConfig)
                            .build())
                .collect(Collectors.toList()));
  }

  public List<AnomalyDetectionConfig> getDefaultModsecDetectionConfigs() {
    return modsecDetectionConfigs;
  }

  public List<AnomalyDetectionConfig> getDefaultApiDefinitionDetectionConfigs() {
    return apiDefinitionDetectionConfigs;
  }

  public List<AnomalyDetectionConfig> getDefaultSessionDefinitionDetectionConfigs() {
    return sessionDefinitionDetectionConfigs;
  }

  public List<AnomalyDetectionConfig> getDefaultCustomRulesDetectionConfigs() {
    return customRulesDetectionConfigs;
  }

  public List<AnomalyDetectionConfig> getDefaultVolumetricDetectionConfigs() {
    return volumetricDetectionConfigs;
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

  private List<AnomalyDetectionConfig> loadDefaultSessionDefinitionDetectionConfigs(
      Config config, List<AnomalyDetectionConfig> sessionDefinitionDetectionRegistryConfigs) {
    List<AnomalyDetectionConfig> detectionConfigs =
        configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(SESSION_DEFINITION_DETECTION_CONFIGS_PATH));

    Map<String, AnomalyDetectionConfig> configMap =
        detectionConfigs.stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getSessionDefinitionMetadataAnomalyDetectionConfig()
                            .getAnomalyRuleId(),
                    anomalyDetectionConfig -> anomalyDetectionConfig));

    return sessionDefinitionDetectionRegistryConfigs.stream()
        .map(
            detectionConfig -> {
              String ruleId =
                  detectionConfig
                      .getSessionDefinitionMetadataAnomalyDetectionConfig()
                      .getAnomalyRuleId();
              if (configMap.containsKey(ruleId)) {
                return detectionConfig.toBuilder().mergeFrom(configMap.get(ruleId)).build();
              }
              return detectionConfig;
            })
        .collect(Collectors.toList());
  }

  private List<AnomalyDetectionConfig> loadDefaultVolumetricDetectionConfigs(
      Config config, List<AnomalyDetectionConfig> volumetricDetectionRegistryConfigs) {
    List<AnomalyDetectionConfig> detectionConfigs =
        configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(VOLUMETRIC_DETECTION_CONFIGS_PATH));

    Map<String, AnomalyDetectionConfig> configMap =
        detectionConfigs.stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getVolumetricAnomalyDetectionConfig()
                            .getAnomalyRuleId(),
                    anomalyDetectionConfig -> anomalyDetectionConfig));

    return volumetricDetectionRegistryConfigs.stream()
        .map(
            detectionConfig -> {
              String ruleId =
                  detectionConfig.getVolumetricAnomalyDetectionConfig().getAnomalyRuleId();
              if (configMap.containsKey(ruleId)) {
                return detectionConfig.toBuilder().mergeFrom(configMap.get(ruleId)).build();
              }
              return detectionConfig;
            })
        .collect(Collectors.toList());
  }
}
