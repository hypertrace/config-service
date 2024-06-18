package ai.traceable.anomaly.config.service.registry.modsec;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ModsecCrsConfig {
  String directivesFilePath;
  String initializationRulesFilePath;
  String rulesFilePath;

  private static final String MODSEC_CRS_RULES_DIRECTORY = "modsec/crs/";
  private static final String DIRECTIVES_DIRECTORY = "directives/";
  private static final String INITIALIZATION_RULES_FILE = "modsec-initialization-901-rules.conf";
  private static final String MODSEC_CRS_RULES_FILE = "modsec-crs-rules.conf";
  private static final String MODSEC_CRS_TEST_RULES_FILE = "modsec-crs-test-rules.conf";
  private static final String MODSEC_CRS_SENSITIVE_AGENT_RULES_FILE =
      "modsec-crs-sensitive-agent-rules.conf";

  public static final Map<ModsecRuleVersion, ModsecCrsConfig> ruleVersionToConfigMap =
      Map.ofEntries(
          Map.entry(
              ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
              getModsecCrsConfig("modsec-directives-v3.conf", MODSEC_CRS_RULES_FILE)),
          Map.entry(
              ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS,
              getModsecCrsConfig("modsec-directives-v3-secarglimits.conf", MODSEC_CRS_RULES_FILE)),
          Map.entry(
              ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE,
              getModsecCrsConfig(
                  "modsec-directives-v3-secarglimits-detectiononly-mode.conf",
                  MODSEC_CRS_RULES_FILE)),
          Map.entry(
              ModsecRuleVersion.MODSEC_RULE_VERSION_TEST_V3,
              getModsecCrsConfig("modsec-directives-v3.conf", MODSEC_CRS_TEST_RULES_FILE)),
          Map.entry(
              ModsecRuleVersion.MODSEC_RULE_VERSION_TEST_V3_SECARG_LIMITS,
              getModsecCrsConfig(
                  "modsec-directives-v3-secarglimits.conf", MODSEC_CRS_TEST_RULES_FILE)),
          Map.entry(
              ModsecRuleVersion.MODSEC_RULE_VERSION_TEST_V3_SECARG_LIMITS_DETECTION_ONLY_MODE,
              getModsecCrsConfig(
                  "modsec-directives-v3-secarglimits-detectiononly-mode.conf",
                  MODSEC_CRS_TEST_RULES_FILE)),
          Map.entry(
              ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3,
              getModsecCrsConfig("coraza-v3-directives.conf", MODSEC_CRS_RULES_FILE)),
          Map.entry(
              ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3_DETECTION_ONLY_MODE,
              getModsecCrsConfig(
                  "coraza-v3-detectiononly-mode-directives.conf", MODSEC_CRS_RULES_FILE)),
          Map.entry(
              ModsecRuleVersion.MODSEC_RULE_VERSION_TEST_CORAZA_V3,
              getModsecCrsConfig("coraza-v3-directives.conf", MODSEC_CRS_TEST_RULES_FILE)),
          Map.entry(
              ModsecRuleVersion.MODSEC_RULE_VERSION_TEST_CORAZA_V3_DETECTION_ONLY_MODE,
              getModsecCrsConfig(
                  "coraza-v3-detectiononly-mode-directives.conf", MODSEC_CRS_TEST_RULES_FILE)),
          Map.entry(
              ModsecRuleVersion.MODSEC_RULE_VERSION_SENSITIVE_AGENT_CORAZA_V3,
              getModsecCrsConfig(
                  "coraza-v3-directives.conf", MODSEC_CRS_SENSITIVE_AGENT_RULES_FILE)),
          Map.entry(
              ModsecRuleVersion.MODSEC_RULE_VERSION_SENSITIVE_AGENT_V3,
              getModsecCrsConfig(
                  "modsec-directives-v3.conf", MODSEC_CRS_SENSITIVE_AGENT_RULES_FILE)));

  private static ModsecCrsConfig getModsecCrsConfig(
      String directivesFile, String modsecCrsRulesFile) {
    return ModsecCrsConfig.builder()
        .directivesFilePath(MODSEC_CRS_RULES_DIRECTORY + DIRECTIVES_DIRECTORY + directivesFile)
        .initializationRulesFilePath(MODSEC_CRS_RULES_DIRECTORY + INITIALIZATION_RULES_FILE)
        .rulesFilePath(MODSEC_CRS_RULES_DIRECTORY + modsecCrsRulesFile)
        .build();
  }
}
