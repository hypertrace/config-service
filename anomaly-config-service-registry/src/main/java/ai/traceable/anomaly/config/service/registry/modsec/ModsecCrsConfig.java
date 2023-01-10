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

  public static final ModsecCrsConfig MODSEC_CRS_V3_CONFIG =
      ModsecCrsConfig.builder()
          .directivesFilePath(MODSEC_CRS_RULES_DIRECTORY + "directives/modsec-directives-v3.conf")
          .initializationRulesFilePath(
              MODSEC_CRS_RULES_DIRECTORY + "modsec-initialization-901-rules.conf")
          .rulesFilePath(MODSEC_CRS_RULES_DIRECTORY + "modsec-crs-rules.conf")
          .build();
  public static final ModsecCrsConfig MODSEC_CRS_V3_SECARG_LIMITS_CONFIG =
      ModsecCrsConfig.builder()
          .directivesFilePath(
              MODSEC_CRS_RULES_DIRECTORY + "directives/modsec-directives-v3-secarglimits.conf")
          .initializationRulesFilePath(
              MODSEC_CRS_RULES_DIRECTORY + "modsec-initialization-901-rules.conf")
          .rulesFilePath(MODSEC_CRS_RULES_DIRECTORY + "modsec-crs-rules.conf")
          .build();

  public static final Map<ModsecRuleVersion, ModsecCrsConfig> ruleVersionToConfigMap =
      Map.of(
          ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
          MODSEC_CRS_V3_CONFIG,
          ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS,
          MODSEC_CRS_V3_SECARG_LIMITS_CONFIG);
}
