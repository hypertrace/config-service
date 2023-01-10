package ai.traceable.anomaly.config.service.registry.modsec;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import java.util.Map;
import java.util.Set;

public interface ModsecRulesRegistry {

  Map<String, AnomalyRuleInfo> getModsecRuleInfos();

  String getModsecCrsRulesBlob(
      AnomalySubRuleType subRuleType,
      ModsecRuleVersion ruleVersion,
      Set<String> disabledModsecRuleIds);

  //  Returns both directives and initalization
  String getModsecHeader(ModsecRuleVersion ruleVersion);
}
