package ai.traceable.anomaly.config.service.registry.modsec;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import java.util.Map;

public interface ModsecRulesRegistry {

  Map<String, AnomalyRuleInfo> getModsecRuleInfos();

  String getModsecSafeRulesBlob();
}
