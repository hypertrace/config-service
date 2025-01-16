package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_IN;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_LIKE;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.List;

public class JexlUtils {

  static MatchCondition getMatchCondition(String jexlExpression) {
    return MatchCondition.newBuilder()
        .setGenericMatchCondition(
            GenericMatchCondition.newBuilder()
                .setJexlExpression(
                    JexlExpressionConfig.newBuilder().setJexlExpression(jexlExpression)))
        .build();
  }

  static String getNotJexlExpression(String jexlExpression) {
    return String.format("!(%s)", jexlExpression);
  }

  static String joinRegexes(List<String> regexList) {
    return String.join("|", regexList);
  }

  static MatchCondition.Builder buildOrMatchConditions(List<MatchCondition> childMatchConditions) {
    MatchCondition.Builder builder;
    if (childMatchConditions.size() == 1) {
      builder = childMatchConditions.get(0).toBuilder();
    } else {
      builder =
          MatchCondition.newBuilder()
              .setLogicalMatchCondition(
                  LogicalMatchCondition.newBuilder()
                      .setOperator(LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR)
                      .addAllConditions(childMatchConditions));
    }
    return builder;
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
