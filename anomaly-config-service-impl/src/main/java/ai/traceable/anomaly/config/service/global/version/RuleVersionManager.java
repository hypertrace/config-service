package ai.traceable.anomaly.config.service.global.version;

import ai.traceable.anomaly.config.service.v1.ChangeLog;
import ai.traceable.anomaly.config.service.v1.RuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersions;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersionsFilter;
import ai.traceable.anomaly.config.service.v1.global.RulesChangeLog;

public interface RuleVersionManager {

  AvailableRuleVersions getAvailableRuleVersions(
      RuleType ruleType, AvailableRuleVersionsFilter filter);

  RulesChangeLog getRulesChangeLog(
      RuleType ruleType, RuleVersion currentVersion, RuleVersion previousVersion);

  ChangeLog getChangeLogDoc(
      RuleType ruleType, RuleVersion currentVersion, RuleVersion previousVersion);
}
