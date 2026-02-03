package ai.traceable.ratelimiting.service.v2.rules;

import ai.traceable.ratelimiting.config.service.v2.BulkDeleteRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.BulkUpdateRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.DeleteRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingEdgeDecisionRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesValidator {
  void validateOrThrow(
      RequestContext requestContext,
      UpdateRateLimitingRuleRequest request,
      List<RateLimitingRule> existingRules);

  void validateOrThrow(RequestContext requestContext, DeleteRateLimitingRuleRequest request);

  void validateOrThrow(
      RequestContext requestContext,
      CreateRateLimitingRuleRequest request,
      List<RateLimitingRule> existingRules);

  void validateOrThrow(RequestContext requestContext, GetRateLimitingRulesRequest request);

  void validateOrThrow(
      RequestContext requestContext, GetRateLimitingEdgeDecisionRulesRequest request);

  void validateOrThrow(
      RequestContext requestContext, GetRateLimitingRuleModsecRulesRequest request);

  void validateOrThrow(RequestContext requestContext, BulkDeleteRateLimitingRulesRequest request);

  void validateOrThrow(RequestContext requestContext, BulkUpdateRateLimitingRulesRequest request);
}
