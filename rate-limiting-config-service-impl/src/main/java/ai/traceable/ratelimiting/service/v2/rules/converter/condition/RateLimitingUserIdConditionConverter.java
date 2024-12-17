package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.edge.decision.config.service.v1.MatchOperator.MATCH_OPERATOR_LIKE;
import static ai.traceable.edge.decision.config.service.v1.MatchOperator.MATCH_OPERATOR_NOT_LIKE;
import static ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase.USER_ID_CONDITION;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.edge.decision.config.service.v1.BinaryOperator;
import ai.traceable.edge.decision.config.service.v1.MatchCondition;
import ai.traceable.edge.decision.config.service.v1.StructuredMatchCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase;
import ai.traceable.ratelimiting.config.service.v2.UserIdCondition;

public class RateLimitingUserIdConditionConverter implements RateLimitingConditionConverter {

  @Override
  public MatchCondition buildMatchCondition(final LeafCondition leafCondition) {
    final UserIdCondition userIdCondition = leafCondition.getUserIdCondition();
    if (!userIdCondition.getUserIdRegexesList().isEmpty()) {
      final StructuredMatchCondition.Builder builder =
          StructuredMatchCondition.newBuilder()
              .setLhs(
                  AttributeDerivationMapping.newBuilder()
                      .setName("lhs")
                      .setType(FIELD_TYPE_STR)
                      .addRules(
                          DerivationRule.newBuilder()
                              .setTransformationConfig(
                                  DataTransformationConfig.newBuilder()
                                      .setJexlExpression(
                                          JexlExpressionConfig.newBuilder()
                                              .setJexlExpression("TRACEABLE_USER_ID")))));
      builder.setBinaryOperator(
          BinaryOperator.newBuilder()
              .setMatchOperator(
                  userIdCondition.getExclude() ? MATCH_OPERATOR_NOT_LIKE : MATCH_OPERATOR_LIKE)
              .setRegex(String.join("|", userIdCondition.getUserIdRegexesList())));
      return MatchCondition.newBuilder().setStructuredMatchCondition(builder).build();
    } else {
      throw new IllegalArgumentException("Unsupported user id scope condition: " + leafCondition);
    }
  }

  @Override
  public ConditionCase getConditionCase() {
    return USER_ID_CONDITION;
  }
}
