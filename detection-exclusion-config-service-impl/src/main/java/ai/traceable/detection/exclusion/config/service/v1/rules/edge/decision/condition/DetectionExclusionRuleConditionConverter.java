package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition;

import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface DetectionExclusionRuleConditionConverter {

  MatchCondition buildMatchCondition(
      RequestContext requestContext, DetectionExclusionCondition condition);

  DetectionExclusionCondition.ConditionCase getConditionCase();
}
