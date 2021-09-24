package ai.traceable.anomaly.config.service.registry.apidef;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionTrainerConfig;
import java.util.Map;

public interface ApiDefinitionRegistry {

  Map<String, AnomalyRuleInfo> getApiDefRuleInfos();

  ApiDefinitionTrainerConfig getApiDefinitionTrainerConfig();
}
