package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import ai.traceable.edge.decision.config.service.v1.MatchCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RateLimitingConditionConverter {

  MatchCondition buildMatchCondition(RequestContext requestContext, LeafCondition leafCondition);

  LeafCondition.ConditionCase getConditionCase();
}
