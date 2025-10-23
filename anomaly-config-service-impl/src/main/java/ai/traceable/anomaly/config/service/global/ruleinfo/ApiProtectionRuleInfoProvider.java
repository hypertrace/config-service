package ai.traceable.anomaly.config.service.global.ruleinfo;

import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import java.util.List;

public interface ApiProtectionRuleInfoProvider {
  List<AnomalyRuleInfo> getApiProtectRuleInfo(
      RuleVersion version, AnomalyEventFamily anomalyEventFamily);

  List<AnomalyRuleInfo> getAllApiProtectRuleInfo(RuleVersion version);
}
