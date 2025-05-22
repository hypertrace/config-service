package ai.traceable.anomaly.config.service.global.version;

import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersions;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersionsFilter;
import ai.traceable.anomaly.config.service.v1.global.RuleType;

public interface RuleVersionManager {

  AvailableRuleVersions getAvailableRuleVersions(
      RuleType ruleType, AvailableRuleVersionsFilter filter);
}
