package ai.traceable.anomaly.config.service.registry.apidef;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import javax.inject.Inject;

public class ApiDefinitionRegistryImpl implements ApiDefinitionRegistry {

  private static final String APIDEF_DIRECTORY = "apidef/";
  private static final String APIDEF_RULE_DETAILS_FILE_PATH =
      APIDEF_DIRECTORY + "apidef-rule-details.conf";
  private static final String APIDEF_RULES_CONFIG_KEY = "apiDefRules";

  private final ConfigConverter configConverter;

  private final Map<String, AnomalyRuleInfo> apiDefRules;

  @Inject
  public ApiDefinitionRegistryImpl(ConfigConverter configConverter) {
    this.configConverter = configConverter;
    this.apiDefRules =
        configConverter.convertAnomalyRuleInfos(
            loadApiDefRuleDetails().getConfigList(APIDEF_RULES_CONFIG_KEY),
            AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF);
  }

  @Override
  public Map<String, AnomalyRuleInfo> getApiDefRuleInfos() {
    return apiDefRules;
  }

  private Config loadApiDefRuleDetails() {
    try {
      return ConfigFactory.parseResources(APIDEF_RULE_DETAILS_FILE_PATH);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to read apiDef rule details file: %s", APIDEF_RULE_DETAILS_FILE_PATH),
          e);
    }
  }
}
