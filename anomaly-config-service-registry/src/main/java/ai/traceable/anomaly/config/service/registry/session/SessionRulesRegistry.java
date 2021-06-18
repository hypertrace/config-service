package ai.traceable.anomaly.config.service.registry.session;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import java.util.Map;

public interface SessionRulesRegistry {

  Map<String, AnomalyRuleInfo> getSessionRuleInfos();
}
