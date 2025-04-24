package ai.traceable.bot.categorized.config.service.v1.translator;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_CONTAINS;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_LIKE;

import ai.traceable.bot.categorized.config.service.v1.UserAgentCondition;
import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;

@UtilityClass
public class UserAgentMatchConditionToEdgeMatchConditionTranslator {

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
                                  .setJexlExpression("$s.getLowerCaseUserAgent()"))))
          .build();

  public static MatchCondition buildMatchCondition(final UserAgentCondition userAgentCondition) {
    if (!userAgentCondition.getUserAgentsList().isEmpty()
        && !userAgentCondition.getUserAgentRegexesList().isEmpty()) {
      return MatchCondition.newBuilder()
          .setLogicalMatchCondition(
              LogicalMatchCondition.newBuilder()
                  .setOperator(LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR)
                  .addAllConditions(getUserAgentMatchConditions(userAgentCondition))
                  .build())
          .build();
    } else if (!userAgentCondition.getUserAgentsList().isEmpty()) {
      return buildContainsOperatorMatchCondition(
          USER_AGENT_LHS, userAgentCondition.getUserAgentsList());
    } else if (!userAgentCondition.getUserAgentRegexesList().isEmpty()) {
      return buildLikeOperatorMatchCondition(
          USER_AGENT_LHS, userAgentCondition.getUserAgentRegexesList());
    }
    return MatchCondition.getDefaultInstance();
  }

  private static Collection<MatchCondition> getUserAgentMatchConditions(
      final UserAgentCondition userAgentCondition) {
    final List<MatchCondition> childMatchConditions = new ArrayList<>();
    if (!userAgentCondition.getUserAgentsList().isEmpty()) {
      childMatchConditions.add(
          buildContainsOperatorMatchCondition(
              USER_AGENT_LHS, userAgentCondition.getUserAgentsList()));
    }
    if (!userAgentCondition.getUserAgentRegexesList().isEmpty()) {
      childMatchConditions.add(
          buildLikeOperatorMatchCondition(
              USER_AGENT_LHS, userAgentCondition.getUserAgentRegexesList()));
    }
    return childMatchConditions;
  }

  public static MatchCondition buildContainsOperatorMatchCondition(
      final AttributeDerivationMapping attributeDerivationMapping, final List<String> values) {
    if (values.size() > 1) {
      return MatchCondition.newBuilder()
          .setLogicalMatchCondition(
              LogicalMatchCondition.newBuilder()
                  .setOperator(LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR)
                  .addAllConditions(
                      values.stream()
                          .map(
                              value ->
                                  MatchCondition.newBuilder()
                                      .setStructuredMatchCondition(
                                          StructuredMatchCondition.newBuilder()
                                              .setLhs(attributeDerivationMapping)
                                              .setBinaryOperator(
                                                  BinaryOperator.newBuilder()
                                                      .setMatchOperator(MATCH_OPERATOR_CONTAINS)
                                                      .setStringValue(value.toLowerCase())))
                                      .build())
                          .collect(Collectors.toList())))
          .build();
    } else {
      return MatchCondition.newBuilder()
          .setStructuredMatchCondition(
              StructuredMatchCondition.newBuilder()
                  .setLhs(attributeDerivationMapping)
                  .setBinaryOperator(
                      BinaryOperator.newBuilder()
                          .setMatchOperator(MATCH_OPERATOR_CONTAINS)
                          .setStringValue(values.get(0).toLowerCase())))
          .build();
    }
  }

  public static MatchCondition buildLikeOperatorMatchCondition(
      final AttributeDerivationMapping attributeDerivationMapping, final List<String> regexes) {
    return MatchCondition.newBuilder()
        .setStructuredMatchCondition(
            StructuredMatchCondition.newBuilder()
                .setLhs(attributeDerivationMapping)
                .setBinaryOperator(
                    BinaryOperator.newBuilder()
                        .setMatchOperator(MATCH_OPERATOR_LIKE)
                        .setRegex(joinRegexes(regexes))))
        .build();
  }

  public static String joinRegexes(final List<String> regexList) {
    return String.join("|", regexList);
  }
}
