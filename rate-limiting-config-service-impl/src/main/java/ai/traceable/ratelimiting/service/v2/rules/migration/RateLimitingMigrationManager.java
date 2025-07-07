package ai.traceable.ratelimiting.service.v2.rules.migration;

import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RateLimitingMigrationManager {
  void migrateFromChangeLog1IfApplicable(RequestContext requestContext);

  CreateRateLimitingRuleRequest migrateCreateRateLimitingRuleRequest(
      CreateRateLimitingRuleRequest createRateLimitingRuleRequest);

  UpdateRateLimitingRuleRequest migrateUpdateRateLimitingRuleRequest(
      UpdateRateLimitingRuleRequest updateRateLimitingRuleRequest);

  void migrateForRuleEvaluationPointsIfApplicable(RequestContext requestContext);
}
