package ai.traceable.ratelimiting.service.v2.rules;

import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.DeleteRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesValidator {
  void validateOrThrow(RequestContext requestContext, UpdateRateLimitingRuleRequest request);

  void validateOrThrow(RequestContext requestContext, DeleteRateLimitingRuleRequest request);

  void validateOrThrow(RequestContext requestContext, CreateRateLimitingRuleRequest request);

  void validateOrThrow(RequestContext requestContext, GetRateLimitingRulesRequest request);
}
