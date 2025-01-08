package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_CONTAINS;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_IN;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_LIKE;
import static ai.traceable.edge.decision.config.service.VariableConstants.USER_ATTRIBUTION_VARIABLE_NAME;

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
import java.util.List;
import java.util.stream.Collectors;

class ConverterUtils {
  static final AttributeDerivationMapping USER_ID_VALUE_LHS =
      AttributeDerivationMapping.newBuilder()
          .setName("lhs")
          .setType(FIELD_TYPE_STR)
          .addRules(
              DerivationRule.newBuilder()
                  .setTransformationConfig(
                      DataTransformationConfig.newBuilder()
                          .setJexlExpression(
                              JexlExpressionConfig.newBuilder()
                                  .setJexlExpression(USER_ATTRIBUTION_VARIABLE_NAME.getValue()))))
          .build();

  static String joinRegexes(List<String> regexList) {
    return String.join("|", regexList);
  }

  static MatchCondition joinChildConditions(
      List<MatchCondition.Builder> childMatchConditions, boolean exclude) {
    MatchCondition.Builder matchCondition;
    if (childMatchConditions.size() == 1) {
      matchCondition = childMatchConditions.get(0);
    } else {
      matchCondition =
          MatchCondition.newBuilder()
              .setLogicalMatchCondition(
                  LogicalMatchCondition.newBuilder()
                      .setOperator(LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR)
                      .addAllConditions(
                          childMatchConditions.stream()
                              .map(MatchCondition.Builder::build)
                              .collect(Collectors.toUnmodifiableList())));
    }
    matchCondition.setNegate(exclude);
    return matchCondition.build();
  }

  static List<MatchCondition.Builder> buildContainsOperatorMatchCondition(
      AttributeDerivationMapping attributeDerivationMapping, List<String> values) {
    return values.stream()
        .map(
            value ->
                MatchCondition.newBuilder()
                    .setStructuredMatchCondition(
                        StructuredMatchCondition.newBuilder()
                            .setLhs(attributeDerivationMapping)
                            .setBinaryOperator(
                                BinaryOperator.newBuilder()
                                    .setMatchOperator(MATCH_OPERATOR_CONTAINS)
                                    .setStringValue(value))))
        .collect(Collectors.toUnmodifiableList());
  }

  static MatchCondition.Builder buildInOperatorMatchCondition(
      AttributeDerivationMapping attributeDerivationMapping, List<String> values) {
    return MatchCondition.newBuilder()
        .setStructuredMatchCondition(
            StructuredMatchCondition.newBuilder()
                .setLhs(attributeDerivationMapping)
                .setBinaryOperator(
                    BinaryOperator.newBuilder()
                        .setMatchOperator(MATCH_OPERATOR_IN)
                        .setListValue(convertToListValue(values))));
  }

  static MatchCondition.Builder buildLikeOperatorMatchCondition(
      AttributeDerivationMapping attributeDerivationMapping, List<String> regexes) {
    return MatchCondition.newBuilder()
        .setStructuredMatchCondition(
            StructuredMatchCondition.newBuilder()
                .setLhs(attributeDerivationMapping)
                .setBinaryOperator(
                    BinaryOperator.newBuilder()
                        .setMatchOperator(MATCH_OPERATOR_LIKE)
                        .setRegex(joinRegexes(regexes))));
  }

  static ListValue convertToListValue(List<String> values) {
    ListValue.Builder listValue = ListValue.newBuilder();
    values.forEach(value -> listValue.addValues(Value.newBuilder().setStringValue(value)));
    return listValue.build();
  }
}
