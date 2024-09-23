package ai.traceable.anomaly.config.service.registry.accounttakeover;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.AccountTakeoverAnomalyDetectionConfig;
import java.util.Map;

public interface AccountTakeoverRulesRegistry {
  Map<String, AnomalyRuleInfo> getAccountTakeoverRuleInfos();

  Map<String, AccountTakeoverAnomalyDetectionConfig> getAccountTakeoverRuleIdToConfigMap();
}
