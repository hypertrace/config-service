package ai.traceable.anomaly.config.service.global.ruleinfo;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import java.util.List;

public interface AiAppRuleInfoProvider {
  List<AnomalyRuleInfo> getAiAppRuleInfo();
}
