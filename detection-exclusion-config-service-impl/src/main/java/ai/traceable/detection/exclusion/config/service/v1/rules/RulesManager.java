package ai.traceable.detection.exclusion.config.service.v1.rules;

import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesManager {

  List<DetectionExclusionRule> getDetectionExclusionRules(
      RequestContext requestContext, GetRulesFilter filter);

  DetectionExclusionRule updateDetectionExclusionRule(
      RequestContext requestContext, DetectionExclusionRule rule);

  DetectionExclusionRule createDetectionExclusionRule(
      RequestContext requestContext,
      DetectionExclusionRuleScope ruleScope,
      DetectionExclusionRuleInfo ruleInfo);

  DetectionExclusionRule deleteDetectionExclusionRule(RequestContext requestContext, String ruleId);
}
