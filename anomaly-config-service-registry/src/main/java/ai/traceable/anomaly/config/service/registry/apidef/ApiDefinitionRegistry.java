package ai.traceable.anomaly.config.service.registry.apidef;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import java.util.Map;

public interface ApiDefinitionRegistry {

  Map<String, AnomalyRuleInfo> getApiDefRuleInfos();
}
