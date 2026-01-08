package ai.traceable.ratelimiting.service.v2.rules;

import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleRecord;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesManager {

  List<RateLimitingRule> getRateLimitingRules(
      RequestContext requestContext, GetRateLimitingRulesFilter filter);

  List<RateLimitingRuleRecord> getRateLimitingRuleRecords(
      RequestContext requestContext, GetRateLimitingRulesFilter filter);

  RateLimitingRule updateRateLimitingRule(
      RequestContext requestContext, String ruleId, RateLimitingRuleData ruleData);

  RateLimitingRule createRateLimitingRule(
      RequestContext requestContext, RateLimitingRuleData ruleData);

  Optional<RateLimitingRule> deleteRateLimitingRule(RequestContext requestContext, String ruleId);

  GetRateLimitingRuleModsecRulesResponse getRateLimitingModsecRules(
      RequestContext requestContext, GetRateLimitingModsecRulesFilter filter);
}
