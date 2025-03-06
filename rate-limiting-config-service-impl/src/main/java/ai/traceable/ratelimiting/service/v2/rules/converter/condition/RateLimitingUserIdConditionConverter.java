package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.edge.decision.converter.utils.ConverterUtils.USER_ID_VALUE_LHS;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.buildInOperatorMatchCondition;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.buildLikeOperatorMatchCondition;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.joinChildConditions;
import static ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase.USER_ID_CONDITION;

import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase;
import ai.traceable.ratelimiting.config.service.v2.UserIdCondition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingUserIdConditionConverter implements RateLimitingConditionConverter {

  @Override
  public MatchConditionDetails buildMatchCondition(
      final RequestContext requestContext, final LeafCondition leafCondition) {
    final UserIdCondition userIdCondition = leafCondition.getUserIdCondition();
    List<MatchCondition.Builder> childMatchConditions = new ArrayList<>();
    if (!userIdCondition.getUserIdsList().isEmpty()) {
      childMatchConditions.add(
          buildInOperatorMatchCondition(USER_ID_VALUE_LHS, userIdCondition.getUserIdsList()));
    }
    if (!userIdCondition.getUserIdRegexesList().isEmpty()) {
      childMatchConditions.add(
          buildLikeOperatorMatchCondition(
              USER_ID_VALUE_LHS, userIdCondition.getUserIdRegexesList()));
    }
    return new MatchConditionDetails(
        joinChildConditions(childMatchConditions, userIdCondition.getExclude()),
        Collections.emptyList(),
        Collections.emptyList());
  }

  @Override
  public ConditionCase getConditionCase() {
    return USER_ID_CONDITION;
  }
}
