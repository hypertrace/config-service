package ai.traceable.anomaly.config.service.registry.credentialstuffing;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.CredentialStuffingAnomalyDetectionConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class CredentialStuffingRulesRegistryImpl implements CredentialStuffingRulesRegistry {
  private static final String CREDENTIAL_STUFFING_DIRECTORY = "credentialstuffing/";
  private static final String CREDENTIAL_STUFFING_RULE_DETAILS_FILE_PATH =
      CREDENTIAL_STUFFING_DIRECTORY + "credential-stuffing-rule-details.yaml";
  private static final String CREDENTIAL_STUFFING_DETECTION_CONFIGS_FILE_PATH =
      CREDENTIAL_STUFFING_DIRECTORY + "credential-stuffing-detection-configs.conf";
  private static final String CREDENTIAL_STUFFING_RULE_ID_TO_CONFIG_MAP_KEY =
      "credentialStuffingRuleIdToConfigMap";
  private final Map<String, AnomalyRuleInfo> credentialStuffingRules;
  private final Map<String, CredentialStuffingAnomalyDetectionConfig>
      credentialStuffingRuleIdToConfigMap;

  @Inject
  public CredentialStuffingRulesRegistryImpl(ConfigConverter configConverter) {
    this.credentialStuffingRules =
        configConverter.getAnomalyRuleInfos(
            CREDENTIAL_STUFFING_RULE_DETAILS_FILE_PATH,
            AnomalyEventFamily.ANOMALY_EVENT_FAMILY_CREDENTIAL_STUFFING);
    this.credentialStuffingRuleIdToConfigMap =
        configConverter
            .convertToAnomalyDetectionConfigs(
                loadCredentialStuffingDetectionConfigs()
                    .getConfigList(CREDENTIAL_STUFFING_RULE_ID_TO_CONFIG_MAP_KEY))
            .stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getCredentialAnomalyDetectionConfig()
                            .getAnomalyRuleId(),
                    AnomalyDetectionConfig::getCredentialAnomalyDetectionConfig));
  }

  @Override
  public Map<String, AnomalyRuleInfo> getCredentialStuffingRuleInfos() {
    return credentialStuffingRules;
  }

  @Override
  public Map<String, CredentialStuffingAnomalyDetectionConfig>
      getCredentialStuffingRuleIdToConfigMap() {
    return credentialStuffingRuleIdToConfigMap;
  }

  private Config loadCredentialStuffingDetectionConfigs() {
    try {
      return ConfigFactory.parseResources(CREDENTIAL_STUFFING_DETECTION_CONFIGS_FILE_PATH);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to read credential stuffing detection configs file: %s",
              CREDENTIAL_STUFFING_DETECTION_CONFIGS_FILE_PATH),
          e);
    }
  }
}
