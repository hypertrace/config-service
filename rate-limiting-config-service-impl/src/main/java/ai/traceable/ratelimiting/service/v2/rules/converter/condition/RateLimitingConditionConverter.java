package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import java.util.Collections;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RateLimitingConditionConverter {

  MatchCondition buildMatchCondition(RequestContext requestContext, LeafCondition leafCondition);

  default List<VariableDerivationMapping> buildVariableDerivationMapping(
      RequestContext requestContext, LeafCondition leafCondition) {
    return Collections.emptyList();
  }

  LeafCondition.ConditionCase getConditionCase();
}
