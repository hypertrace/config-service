package ai.traceable.anomaly.config.service.registry.modsec;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class ModsecRulesRegistryImpl implements ModsecRulesRegistry {
  private static final String NEWLINE_DELIMITER = "\n";
  private static final String MODSEC_DIRECTORY = "modsec/";
  private static final String MODSEC_RULE_DETAILS_FILE_PATH =
      MODSEC_DIRECTORY + "modsec-rule-details.conf";

  private static final String MODSEC_CRS_RULES_DIRECTORY = MODSEC_DIRECTORY + "crs/";
  private static final String MODSEC_CRS_DIRECTIVES_FILE_PATH =
      MODSEC_CRS_RULES_DIRECTORY + "modsec-directives.conf";
  private static final String MODSEC_CRS_INITIALIZATION_RULES_FILE_PATH =
      MODSEC_CRS_RULES_DIRECTORY + "modsec-initialization-901-rules.conf";
  private static final String MODSEC_CRS_RULES_FILE_PATH =
      MODSEC_CRS_RULES_DIRECTORY + "modsec-crs-rules.conf";
  private static final String MODSEC_RULES_CONFIG_KEY = "modsecRules";

  private static final List<AnomalySubRuleType> SUPPORTED_SUB_RULE_TYPES =
      ImmutableList.of(
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_UNSAFE,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK);

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
  public String getModsecCrsRulesBlob(
      AnomalySubRuleType subRuleType, Set<String> disabledModsecRuleIds) {
    if (!SUPPORTED_SUB_RULE_TYPES.contains(subRuleType)) {
      throw new IllegalArgumentException(
          String.format("Invalid SubRuleType %s to fetch Modsec CRS rules", subRuleType));
    }
    return modsecCrsRulesHandler.getModsecCrsBlob(
        MODSEC_CRS_DIRECTIVES_FILE_PATH,
        MODSEC_CRS_INITIALIZATION_RULES_FILE_PATH,
        MODSEC_CRS_RULES_FILE_PATH,
        modsecRules,
        subRuleType,
        disabledModsecRuleIds);
  }

  @Override
  public String getModsecHeader() {
    return String.join(
        NEWLINE_DELIMITER,
        modsecCrsRulesHandler.loadModsecCrsFileContents(MODSEC_CRS_DIRECTIVES_FILE_PATH),
        modsecCrsRulesHandler.loadModsecCrsFileContents(MODSEC_CRS_INITIALIZATION_RULES_FILE_PATH));
  }

  private Map<String, AnomalyRuleInfo> initModsecRules() {
    Map<String, AnomalyRuleInfo.Builder> anomalyRuleBuildersMap = new HashMap<>();

    configConverter
        .convertAnomalyRuleInfos(
            loadModsecRuleDetails().getConfigList(MODSEC_RULES_CONFIG_KEY),
            AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
        .forEach((id, rule) -> anomalyRuleBuildersMap.put(id, rule.toBuilder()));

    modsecCrsRulesHandler
        .parseModsecCrsRules(
            modsecCrsRulesHandler.loadModsecCrsFileContents(MODSEC_CRS_RULES_FILE_PATH))
        .forEach((id, subRules) -> anomalyRuleBuildersMap.get(id).addAllSubRuleInfos(subRules));

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
