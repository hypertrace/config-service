package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase.USER_ID_CONDITION;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.ConverterUtils.USER_ID_VALUE_LHS;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.ConverterUtils.buildInOperatorMatchCondition;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.ConverterUtils.buildLikeOperatorMatchCondition;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.ConverterUtils.joinChildConditions;

import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase;
import ai.traceable.ratelimiting.config.service.v2.UserIdCondition;
import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingUserIdConditionConverter implements RateLimitingConditionConverter {

  @Override
  public MatchCondition buildMatchCondition(
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
    return joinChildConditions(childMatchConditions, userIdCondition.getExclude());
  }

  @Override
  public ConditionCase getConditionCase() {
    return USER_ID_CONDITION;
  }
}
