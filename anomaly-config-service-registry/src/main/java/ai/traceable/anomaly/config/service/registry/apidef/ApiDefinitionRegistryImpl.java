package ai.traceable.anomaly.config.service.registry.apidef;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.stream.Collectors;

public class ApiDefinitionRegistryImpl implements ApiDefinitionRegistry {

  private static final String APIDEF_DIRECTORY = "apidef/";
  private static final String APIDEF_RULE_DETAILS_FILE_PATH =
      APIDEF_DIRECTORY + "apidef-rule-details.yaml";
  private static final String APIDEF_DETECTION_CONFIGS_FILE_PATH =
      APIDEF_DIRECTORY + "apidef-detection-configs.conf";
  private static final String APIDEF_RULE_ID_TO_CONFIG_MAP_KEY = "apiDefRuleIdToConfigMap";

  private final Map<String, AnomalyRuleInfo> apiDefRules;

  private final Map<String, ApiDefinitionMetadataAnomalyDetectionConfig> apiDefRuleIdToConfigMap;

  @Inject
  public ApiDefinitionRegistryImpl(ConfigConverter configConverter) {
    this.apiDefRules =
        configConverter.getAnomalyRuleInfos(
            APIDEF_RULE_DETAILS_FILE_PATH, AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF);
    this.apiDefRuleIdToConfigMap =
        configConverter
            .convertToAnomalyDetectionConfigs(
                loadApiDefDetectionConfigs().getConfigList(APIDEF_RULE_ID_TO_CONFIG_MAP_KEY))
            .stream()
            .map(AnomalyDetectionConfig::getApiDefinitionMetadataAnomalyDetectionConfig)
            .collect(
                Collectors.toMap(
                    ApiDefinitionMetadataAnomalyDetectionConfig::getAnomalyRuleId,
                    detectionConfig -> detectionConfig));
  }

  @Override
  public Map<String, AnomalyRuleInfo> getApiDefRuleInfos() {
    return apiDefRules;
  }

  @Override
  public Map<String, ApiDefinitionMetadataAnomalyDetectionConfig>
      getApiDefRuleIdToDetectionConfigMap() {
    return apiDefRuleIdToConfigMap;
  }

  private Config loadApiDefDetectionConfigs() {
    try {
      return ConfigFactory.parseResources(APIDEF_DETECTION_CONFIGS_FILE_PATH);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to read apiDef detection configs file: %s",
              APIDEF_DETECTION_CONFIGS_FILE_PATH),
          e);
    }
  }
}
