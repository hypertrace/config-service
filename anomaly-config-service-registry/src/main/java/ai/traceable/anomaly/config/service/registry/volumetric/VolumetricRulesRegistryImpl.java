package ai.traceable.anomaly.config.service.registry.volumetric;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.VolumetricAnomalyDetectionConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class VolumetricRulesRegistryImpl implements VolumetricRulesRegistry {
  private static final String VOLUMETRIC_DIRECTORY = "volumetric/";
  private static final String VOLUMETRIC_RULE_DETAILS_FILE_PATH =
      VOLUMETRIC_DIRECTORY + "volumetric-rule-details.yaml";
  private static final String VOLUMETRIC_DETECTION_CONFIGS_FILE_PATH =
      VOLUMETRIC_DIRECTORY + "volumetric-detection-configs.conf";
  private static final String VOLUMETRIC_RULE_ID_TO_CONFIG_MAP_KEY = "volumetricRuleIdToConfigMap";
  private final Map<String, AnomalyRuleInfo> volumetricRules;
  private final Map<String, VolumetricAnomalyDetectionConfig> volumetricRuleIdToConfigMap;

  @Inject
  public VolumetricRulesRegistryImpl(ConfigConverter configConverter) {
    this.volumetricRules =
        configConverter.getAnomalyRuleInfos(
            VOLUMETRIC_RULE_DETAILS_FILE_PATH, AnomalyEventFamily.ANOMALY_EVENT_FAMILY_VOLUMETRIC);
    this.volumetricRuleIdToConfigMap =
        configConverter
            .convertToAnomalyDetectionConfigs(
                loadVolumetricDetectionConfigs()
                    .getConfigList(VOLUMETRIC_RULE_ID_TO_CONFIG_MAP_KEY))
            .stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getVolumetricAnomalyDetectionConfig()
                            .getAnomalyRuleId(),
                    AnomalyDetectionConfig::getVolumetricAnomalyDetectionConfig));
  }

  @Override
  public Map<String, AnomalyRuleInfo> getVolumetricRuleInfos() {
    return volumetricRules;
  }

  @Override
  public Map<String, VolumetricAnomalyDetectionConfig> getVolumetricRuleIdToConfigMap() {
    return volumetricRuleIdToConfigMap;
  }

  private Config loadVolumetricDetectionConfigs() {
    try {
      return ConfigFactory.parseResources(VOLUMETRIC_DETECTION_CONFIGS_FILE_PATH);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to read session detection configs file: %s",
              VOLUMETRIC_DETECTION_CONFIGS_FILE_PATH),
          e);
    }
  }
}
