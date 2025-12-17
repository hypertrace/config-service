package ai.traceable.anomaly.config.service.global.ruleinfo;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import java.util.List;
import java.util.Set;

public interface WebAppRuleInfoProvider {

  List<AnomalyRuleInfo> getWebAppRuleInfo(RuleVersion version);

  String getCrsRulesBlob(
      List<AnomalySubRuleType> subRuleTypes,
      ModsecRuleVersion ruleVersion,
      Set<String> disabledRuleIds,
      RuleVersion version,
      boolean includeDirectives);

  String getImpactScoringBlob(RuleVersion version);
}
