package ai.traceable.anomaly.config.service.registry.modsec;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import java.util.Map;

public interface ModsecRulesRegistry {

  Map<String, AnomalyRuleInfo> getModsecRuleInfos();

  String getModsecCrsRulesBlob(AnomalySubRuleType subRuleType);

  //  Returns both directives and initalization
  String getModsecHeader();
}
