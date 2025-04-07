package ai.traceable.bot.categorized.config.service.v1.translator;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_IN;
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
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
                                  .setJexlExpression("$s.getUserAgent()"))))
          .build();

  public static MatchCondition buildMatchCondition(final UserAgentCondition userAgentCondition) {
    return MatchCondition.newBuilder()
        .setLogicalMatchCondition(
            LogicalMatchCondition.newBuilder()
                .setOperator(LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR)
                .addAllConditions(getUserAgentMatchConditions(userAgentCondition))
                .build())
        .build();
  }

  private static Collection<MatchCondition> getUserAgentMatchConditions(
      final UserAgentCondition userAgentCondition) {
    List<MatchCondition> childMatchConditions = new ArrayList<>();
    if (!userAgentCondition.getUserAgentsList().isEmpty()) {
      childMatchConditions.add(
          buildInOperatorMatchCondition(USER_AGENT_LHS, userAgentCondition.getUserAgentsList()));
    }
    if (!userAgentCondition.getUserAgentRegexesList().isEmpty()) {
      childMatchConditions.add(
          buildLikeOperatorMatchCondition(
              USER_AGENT_LHS, userAgentCondition.getUserAgentRegexesList()));
    }
    return childMatchConditions;
  }

  public static MatchCondition buildInOperatorMatchCondition(
      AttributeDerivationMapping attributeDerivationMapping, List<String> values) {
    return MatchCondition.newBuilder()
        .setStructuredMatchCondition(
            StructuredMatchCondition.newBuilder()
                .setLhs(attributeDerivationMapping)
                .setBinaryOperator(
                    BinaryOperator.newBuilder()
                        .setMatchOperator(MATCH_OPERATOR_IN)
                        .setListValue(convertToListValue(values))))
        .build();
  }

  public static MatchCondition buildLikeOperatorMatchCondition(
      AttributeDerivationMapping attributeDerivationMapping, List<String> regexes) {
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

  public static ListValue convertToListValue(List<String> values) {
    ListValue.Builder listValue = ListValue.newBuilder();
    values.forEach(value -> listValue.addValues(Value.newBuilder().setStringValue(value)));
    return listValue.build();
  }

  public static String joinRegexes(List<String> regexList) {
    return String.join("|", regexList);
  }
}
