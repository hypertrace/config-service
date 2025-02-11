package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase.USER_AGENT_CONDITION;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.ConverterUtils.buildInOperatorMatchCondition;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.ConverterUtils.buildLikeOperatorMatchCondition;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.ConverterUtils.joinChildConditions;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase;
import ai.traceable.ratelimiting.config.service.v2.UserAgentCondition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingUserAgentConditionConverter implements RateLimitingConditionConverter {

  private static final String USER_AGENT_JEXL_EXP = "$s.getUserAgent()";

  private static final AttributeDerivationMapping USER_AGENT_LHS =
      AttributeDerivationMapping.newBuilder()
          .setName("lhs")
          .setType(FIELD_TYPE_STR)
          .addRules(
              DerivationRule.newBuilder()
                  .setTransformationConfig(
                      DataTransformationConfig.newBuilder()
                          .setOutputType(FIELD_TYPE_STR)
                          .setJexlExpression(
                              JexlExpressionConfig.newBuilder()
                                  .setJexlExpression(USER_AGENT_JEXL_EXP))))
          .build();

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
