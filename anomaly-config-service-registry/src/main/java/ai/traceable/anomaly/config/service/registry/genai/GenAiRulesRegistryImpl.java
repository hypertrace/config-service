package ai.traceable.anomaly.config.service.registry.genai;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.GenAiAnomalyDetectionConfig;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import java.util.stream.Collectors;

public class GenAiRulesRegistryImpl implements GenAiRulesRegistry {

  private static final String GEN_AI_DIRECTORY = "genai/";
  private static final String GEN_AI_DETECTION_CONFIGS_FILE_PATH =
      GEN_AI_DIRECTORY + "genai-detection-configs.conf";
  private static final String GEN_AI_RULE_ID_TO_CONFIG_MAP_KEY = "genAiRuleIdToConfigMap";

  private final Map<String, GenAiAnomalyDetectionConfig> genAiRuleIdToConfigMap;

  @Inject
  public GenAiRulesRegistryImpl(ConfigConverter configConverter) {
    this.genAiRuleIdToConfigMap =
        configConverter
            .convertToAnomalyDetectionConfigs(
                loadGenAiDetectionConfigs().getConfigList(GEN_AI_RULE_ID_TO_CONFIG_MAP_KEY))
            .stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig.getGenAiAnomalyDetectionConfig().getAnomalyRuleId(),
                    AnomalyDetectionConfig::getGenAiAnomalyDetectionConfig));
  }

  @Override
  public Map<String, GenAiAnomalyDetectionConfig> getGenAiRuleIdToConfigMap() {
    return genAiRuleIdToConfigMap;
  }

  private Config loadGenAiDetectionConfigs() {
    try {
      return ConfigFactory.parseResources(GEN_AI_DETECTION_CONFIGS_FILE_PATH);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to read genAi detection configs file: %s",
              GEN_AI_DETECTION_CONFIGS_FILE_PATH),
          e);
    }
  }
}
