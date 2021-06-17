package ai.traceable.anomaly.config.service.registry.apidef;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import java.util.Map;

public interface ApiDefRulesRegistry {

  Map<String, AnomalyRuleInfo> getApiDefRuleConfigs();
}
