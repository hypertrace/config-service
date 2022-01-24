package ai.traceable.anomaly.config.service.registry.session;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class SessionRulesRegistryImpl implements SessionRulesRegistry {

  private static final String SESSION_DIRECTORY = "session/";
  private static final String SESSION_RULE_DETAILS_FILE_PATH =
      SESSION_DIRECTORY + "session-rule-details.conf";
  private static final String SESSION_RULES_CONFIG_KEY = "sessionRules";
  private static final String SESSION_DETECTION_CONFIGS_FILE_PATH =
      SESSION_DIRECTORY + "session-detection-configs.conf";
  private static final String SESSION_RULE_ID_TO_CONFIG_MAP_KEY = "sessionDefRuleIdToConfigMap";

  private final ConfigConverter configConverter;

  private final Map<String, AnomalyRuleInfo> sessionRules;
  private final Map<String, SessionDefinitionMetadataAnomalyDetectionConfig>
      sessionDefRuleIdToConfigMap;

  @Inject
  public SessionRulesRegistryImpl(ConfigConverter configConverter) {
    this.configConverter = configConverter;
    this.sessionRules =
        configConverter.convertAnomalyRuleInfos(
            loadSessionRuleDetails().getConfigList(SESSION_RULES_CONFIG_KEY),
            AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION);
    this.sessionDefRuleIdToConfigMap =
        configConverter
            .convertToAnomalyDetectionConfigs(
                loadSessionDetectionConfigs().getConfigList(SESSION_RULE_ID_TO_CONFIG_MAP_KEY))
            .stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getSessionDefinitionMetadataAnomalyDetectionConfig()
                            .getAnomalyRuleId(),
                    AnomalyDetectionConfig::getSessionDefinitionMetadataAnomalyDetectionConfig));
  }

  @Override
  public Map<String, AnomalyRuleInfo> getSessionRuleInfos() {
    return sessionRules;
  }

  @Override
  public Map<String, SessionDefinitionMetadataAnomalyDetectionConfig>
      getSessionDefRuleIdToDetectionConfigMap() {
    return sessionDefRuleIdToConfigMap;
  }

  private Config loadSessionRuleDetails() {
    try {
      return ConfigFactory.parseResources(SESSION_RULE_DETAILS_FILE_PATH);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to read session rule details file: %s", SESSION_RULE_DETAILS_FILE_PATH),
          e);
    }
  }

  private Config loadSessionDetectionConfigs() {
    try {
      return ConfigFactory.parseResources(SESSION_DETECTION_CONFIGS_FILE_PATH);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to read session detection configs file: %s",
              SESSION_DETECTION_CONFIGS_FILE_PATH),
          e);
    }
  }
}
