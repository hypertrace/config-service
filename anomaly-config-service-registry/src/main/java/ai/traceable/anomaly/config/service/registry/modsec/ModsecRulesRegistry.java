package ai.traceable.anomaly.config.service.registry.modsec;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ModsecRulesRegistry {
  // handle deprecated enums
  String TEST_VERSION_KEYWORD = "TEST_";

  Map<String, AnomalyRuleInfo> getModsecRuleInfos(
      ModsecRuleVersion modsecRuleVersion, boolean useTestRules);

  String getModsecCrsRulesBlob(
      List<AnomalySubRuleType> subRuleTypes,
      ModsecRuleVersion ruleVersion,
      Set<String> disabledModsecRuleIds,
      boolean useTestRules);

  //  Returns both directives and initialization
  String getModsecHeader(ModsecRuleVersion ruleVersion);

  // handle deprecated enums
  default boolean isModsecTestRuleVersion(ModsecRuleVersion ruleVersion) {
    return ruleVersion.name().contains(TEST_VERSION_KEYWORD);
  }

  default ModsecRuleVersion getTestStrippedVersion(ModsecRuleVersion ruleVersion) {
    return ModsecRuleVersion.valueOf(ruleVersion.name().replace(TEST_VERSION_KEYWORD, ""));
  }
}
