package ai.traceable.anomaly.config.service.registry.modsec;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class ModsecRulesRegistryImpl implements ModsecRulesRegistry {

  private static final String MODSEC_DIRECTORY = "modsec/";
  private static final String MODSEC_RULE_DETAILS_FILE_PATH =
      MODSEC_DIRECTORY + "modsec-rule-details.conf";

  private static final String MODSEC_CRS_RULES_DIRECTORY = MODSEC_DIRECTORY + "crs/";
  private static final String MODSEC_CRS_DIRECTIVES_FILE_PATH =
      MODSEC_CRS_RULES_DIRECTORY + "modsec-directives.conf";
  private static final String MODSEC_CRS_INITIALIZATION_RULES_FILE_PATH =
      MODSEC_CRS_RULES_DIRECTORY + "modsec-initialization-901-rules.conf";
  private static final String MODSEC_RULES_CONFIG_KEY = "modsecRules";
  private static final String MODSEC_CRS_SAFE_RULES_FILE_PATH =
      MODSEC_CRS_RULES_DIRECTORY + "modsec-safe-rules.conf";
  private static final String MODSEC_CRS_REGULAR_RULES_FILE_PATH =
      MODSEC_CRS_RULES_DIRECTORY + "modsec-regular-rules.conf";

  private final ConfigConverter configConverter;
  private final ModsecCrsRulesHandler modsecCrsRulesHandler;

  private final Map<String, AnomalyRuleInfo> modsecRules;

  @Inject
  public ModsecRulesRegistryImpl(
      ConfigConverter configConverter, ModsecCrsRulesHandler modsecCrsRulesHandler) {
    this.configConverter = configConverter;
    this.modsecCrsRulesHandler = modsecCrsRulesHandler;
    this.modsecRules = initModsecRules();
  }

  @Override
  public Map<String, AnomalyRuleInfo> getModsecRuleInfos() {
    return modsecRules;
  }

  @Override
  public String getModsecSafeCrsRulesBlob() {
    return modsecCrsRulesHandler.getModsecCrsBlob(
        MODSEC_CRS_DIRECTIVES_FILE_PATH,
        MODSEC_CRS_INITIALIZATION_RULES_FILE_PATH,
        MODSEC_CRS_SAFE_RULES_FILE_PATH);
  }

  @Override
  public String getModsecRegularCrsRulesBlob() {
    return modsecCrsRulesHandler.getModsecCrsBlob(
        MODSEC_CRS_DIRECTIVES_FILE_PATH,
        MODSEC_CRS_INITIALIZATION_RULES_FILE_PATH,
        MODSEC_CRS_REGULAR_RULES_FILE_PATH);
  }

  private Map<String, AnomalyRuleInfo> initModsecRules() {
    Map<String, AnomalyRuleInfo.Builder> anomalyRuleBuildersMap = new HashMap<>();

    configConverter
        .convertAnomalyRuleInfos(
            loadModsecRuleDetails().getConfigList(MODSEC_RULES_CONFIG_KEY),
            AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
        .forEach((id, rule) -> anomalyRuleBuildersMap.put(id, rule.toBuilder()));

    Map<String, Map<String, String>> modsecCrsRegularRulesMap =
        modsecCrsRulesHandler.parseModsecCrsRules(
            modsecCrsRulesHandler.loadModsecCrsFileContents(MODSEC_CRS_REGULAR_RULES_FILE_PATH));

    Map<String, Map<String, String>> modsecCrsSafeRulesMap =
        modsecCrsRulesHandler.parseModsecCrsRules(
            modsecCrsRulesHandler.loadModsecCrsFileContents(MODSEC_CRS_SAFE_RULES_FILE_PATH));

    // safe-rules are available for blocking..
    modsecCrsSafeRulesMap.forEach(
        (ruleId, subRulesMap) ->
            subRulesMap.forEach(
                (id, name) ->
                    anomalyRuleBuildersMap
                        .get(ruleId)
                        .addSubRuleInfos(
                            AnomalySubRuleInfo.newBuilder()
                                .setRuleId(id)
                                .setRuleName(name)
                                .setBlockingAvailable(true))));

    // do not over-ride safe-rules with regular rules
    modsecCrsRegularRulesMap.forEach(
        (ruleId, subRulesMap) ->
            subRulesMap.forEach(
                (id, name) -> {
                  if (!modsecCrsSafeRulesMap.containsKey(ruleId)
                      || !modsecCrsSafeRulesMap.get(ruleId).containsKey(id)) {
                    if (!anomalyRuleBuildersMap.containsKey(ruleId)) {
                      throw new RuntimeException("No rule details added for ruleId:" + ruleId);
                    }
                    anomalyRuleBuildersMap
                        .get(ruleId)
                        .addSubRuleInfos(
                            AnomalySubRuleInfo.newBuilder().setRuleId(id).setRuleName(name));
                  }
                }));

    return anomalyRuleBuildersMap.values().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                AnomalyRuleInfo.Builder::getRuleId, AnomalyRuleInfo.Builder::build));
  }

  private Config loadModsecRuleDetails() {
    try {
      return ConfigFactory.parseResources(MODSEC_RULE_DETAILS_FILE_PATH);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to read modsec rule details file: %s", MODSEC_RULE_DETAILS_FILE_PATH),
          e);
    }
  }
}
