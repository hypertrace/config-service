package ai.traceable.anomaly.config.service.registry.modsec;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ModsecRulesRegistry {

  Map<String, AnomalyRuleInfo> getModsecRuleInfos(ModsecRuleVersion modsecRuleVersion);

  String getModsecCrsRulesBlob(
      List<AnomalySubRuleType> subRuleTypes,
      ModsecRuleVersion ruleVersion,
      Set<String> disabledModsecRuleIds);

  //  Returns both directives and initialization
  String getModsecHeader(ModsecRuleVersion ruleVersion);
}
