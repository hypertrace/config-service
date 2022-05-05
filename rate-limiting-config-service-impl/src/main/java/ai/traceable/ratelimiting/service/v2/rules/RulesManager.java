package ai.traceable.ratelimiting.service.v2.rules;

import ai.traceable.ratelimiting.config.service.v2.GetRulesByCategoryFilter;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesManager {
  List<RateLimitingRule> getRateLimitingRules(
      RequestContext requestContext, GetRulesByCategoryFilter filter);

  RateLimitingRule updateRateLimitingRule(
      RequestContext requestContext, String ruleId, RateLimitingRuleData ruleData);

  RateLimitingRule createRateLimitingRule(
      RequestContext requestContext, RateLimitingRuleData ruleData);

  RateLimitingRule deleteRateLimitingRule(RequestContext requestContext, String ruleId);
}
