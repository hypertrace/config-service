package ai.traceable.anomaly.config.service.global.version;

import ai.traceable.anomaly.config.service.v1.RuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersions;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersionsFilter;
import ai.traceable.anomaly.config.service.v1.global.RulesChangeLog;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RuleVersionManager {

  AvailableRuleVersions getAvailableRuleVersions(
      RuleType ruleType, AvailableRuleVersionsFilter filter);

  RulesChangeLog getRulesChangeLog(
      RequestContext requestContext,
      RuleType ruleType,
      RuleVersion currentVersion,
      RuleVersion previousVersion);
}
