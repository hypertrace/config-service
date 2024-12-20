package ai.traceable.anomaly.config.service.registry.accounttakeover;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.AccountTakeoverAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.stream.Collectors;

public class AccountTakeoverRulesRegistryImpl implements AccountTakeoverRulesRegistry {
  private static final String ACCOUNT_TAKEOVER_DIRECTORY = "accounttakeover/";
  private static final String ACCOUNT_TAKEOVER_RULE_DETAILS_FILE_PATH =
      ACCOUNT_TAKEOVER_DIRECTORY + "account-takeover-rule-details.yaml";
  private static final String ACCOUNT_TAKEOVER_DETECTION_CONFIGS_FILE_PATH =
      ACCOUNT_TAKEOVER_DIRECTORY + "account-takeover-detection-configs.conf";
  private static final String ACCOUNT_TAKEOVER_RULE_ID_TO_CONFIG_MAP_KEY =
      "accountTakeoverRuleIdToConfigMap";
  private final Map<String, AnomalyRuleInfo> accountTakeoverRules;
  private final Map<String, AccountTakeoverAnomalyDetectionConfig> accountTakeoverRuleIdToConfigMap;

  @Inject
  public AccountTakeoverRulesRegistryImpl(ConfigConverter configConverter) {
    this.accountTakeoverRules =
        configConverter.getAnomalyRuleInfos(
            ACCOUNT_TAKEOVER_RULE_DETAILS_FILE_PATH,
            AnomalyEventFamily.ANOMALY_EVENT_FAMILY_CREDENTIAL_STUFFING);
    this.accountTakeoverRuleIdToConfigMap =
        configConverter
            .convertToAnomalyDetectionConfigs(
                loadAccountTakeoverDetectionConfigs()
                    .getConfigList(ACCOUNT_TAKEOVER_RULE_ID_TO_CONFIG_MAP_KEY))
            .stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getAccountTakeoverAnomalyDetectionConfig()
                            .getAnomalyRuleId(),
                    AnomalyDetectionConfig::getAccountTakeoverAnomalyDetectionConfig));
  }

  @Override
  public Map<String, AnomalyRuleInfo> getAccountTakeoverRuleInfos() {
    return accountTakeoverRules;
  }

  @Override
  public Map<String, AccountTakeoverAnomalyDetectionConfig> getAccountTakeoverRuleIdToConfigMap() {
    return accountTakeoverRuleIdToConfigMap;
  }

  private Config loadAccountTakeoverDetectionConfigs() {
    try {
      return ConfigFactory.parseResources(ACCOUNT_TAKEOVER_DETECTION_CONFIGS_FILE_PATH);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to read account takeover detection configs file: %s",
              ACCOUNT_TAKEOVER_DETECTION_CONFIGS_FILE_PATH),
          e);
    }
  }
}
