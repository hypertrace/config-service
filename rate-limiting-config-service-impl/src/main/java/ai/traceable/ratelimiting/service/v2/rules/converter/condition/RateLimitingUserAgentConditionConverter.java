package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.edge.decision.converter.utils.ConverterUtils.USER_AGENT_LHS;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.buildInOperatorMatchCondition;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.buildLikeOperatorMatchCondition;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.joinChildConditions;
import static ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase.USER_AGENT_CONDITION;

import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase;
import ai.traceable.ratelimiting.config.service.v2.UserAgentCondition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingUserAgentConditionConverter implements RateLimitingConditionConverter {

  @Override
  public MatchConditionDetails buildMatchCondition(
      final RequestContext requestContext, final LeafCondition leafCondition) {
    final UserAgentCondition userAgentCondition = leafCondition.getUserAgentCondition();
    List<MatchCondition.Builder> childMatchConditions = new ArrayList<>();
    if (!userAgentCondition.getUserAgentsList().isEmpty()) {
      childMatchConditions.add(
          buildInOperatorMatchCondition(USER_AGENT_LHS, userAgentCondition.getUserAgentsList()));
    }
    if (!userAgentCondition.getUserAgentRegexesList().isEmpty()) {
      childMatchConditions.add(
          buildLikeOperatorMatchCondition(
              USER_AGENT_LHS, userAgentCondition.getUserAgentRegexesList()));
    }
    return new MatchConditionDetails(
        joinChildConditions(childMatchConditions, userAgentCondition.getExclude()),
        Collections.emptyList(),
        Collections.emptyList());
  }

  @Override
  public ConditionCase getConditionCase() {
    return USER_AGENT_CONDITION;
  }
}
