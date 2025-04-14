package ai.traceable.edge.decision.converter.utils;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_CONTAINS;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_IN;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_LIKE;
import static ai.traceable.edge.decision.config.service.VariableConstants.USER_ATTRIBUTION_VARIABLE_NAME;
import static ai.traceable.edge.decision.converter.utils.Constants.ATTRIBUTE_NAME_LHS;
import static ai.traceable.edge.decision.converter.utils.Constants.IP_ADDRESS_JEXL_EXP;
import static ai.traceable.edge.decision.converter.utils.Constants.PATH_JEXL_EXP;
import static ai.traceable.edge.decision.converter.utils.Constants.SERVICE_JEXL_EXP;
import static ai.traceable.edge.decision.converter.utils.Constants.USER_AGENT_JEXL_EXP;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.List;
import java.util.stream.Collectors;

public class ConverterUtils {

  private ConverterUtils() {
    // utility classes shouldn't have a public constructor
  }

  public static final AttributeDerivationMapping USER_ID_VALUE_LHS =
      AttributeDerivationMapping.newBuilder()
          .setName(ATTRIBUTE_NAME_LHS)
          .setType(FIELD_TYPE_STR)
          .addRules(
              DerivationRule.newBuilder()
                  .setTransformationConfig(
                      DataTransformationConfig.newBuilder()
                          .setOutputType(FIELD_TYPE_STR)
                          .setJexlExpression(
                              JexlExpressionConfig.newBuilder()
                                  .setJexlExpression(USER_ATTRIBUTION_VARIABLE_NAME.getValue()))))
          .build();

  public static final AttributeDerivationMapping USER_AGENT_LHS =
      AttributeDerivationMapping.newBuilder()
          .setName(ATTRIBUTE_NAME_LHS)
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

  public static final AttributeDerivationMapping IP_ADDRESS_ATTRIBUTE =
      AttributeDerivationMapping.newBuilder()
          .setName(ATTRIBUTE_NAME_LHS)
          .setType(FieldType.FIELD_TYPE_STR)
          .addRules(
              DerivationRule.newBuilder()
                  .setTransformationConfig(
                      DataTransformationConfig.newBuilder()
                          .setJexlExpression(
                              JexlExpressionConfig.newBuilder()
                                  .setJexlExpression(IP_ADDRESS_JEXL_EXP))))
          .build();

  public static final AttributeDerivationMapping PATH_ATTRIBUTE =
      AttributeDerivationMapping.newBuilder()
          .setName(ATTRIBUTE_NAME_LHS)
          .setType(FIELD_TYPE_STR)
          .addRules(
              DerivationRule.newBuilder()
                  .setTransformationConfig(
                      DataTransformationConfig.newBuilder()
                          .setJexlExpression(
                              JexlExpressionConfig.newBuilder().setJexlExpression(PATH_JEXL_EXP))))
          .build();

  public static final AttributeDerivationMapping SERVICE_ATTRIBUTE =
      AttributeDerivationMapping.newBuilder()
          .setName(ATTRIBUTE_NAME_LHS)
          .setType(FIELD_TYPE_STR)
          .addRules(
              DerivationRule.newBuilder()
                  .setTransformationConfig(
                      DataTransformationConfig.newBuilder()
                          .setJexlExpression(
                              JexlExpressionConfig.newBuilder()
                                  .setJexlExpression(SERVICE_JEXL_EXP))))
          .build();

  public static String joinRegexes(List<String> regexList) {
    return String.join("|", regexList);
  }

  public static MatchCondition joinChildConditions(
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

  public static MatchCondition.Builder buildOrMatchConditions(
      List<MatchCondition> childMatchConditions) {
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

  public static List<MatchCondition.Builder> buildContainsOperatorMatchCondition(
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

  public static MatchCondition.Builder buildInOperatorMatchCondition(
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

  public static MatchCondition.Builder buildLikeOperatorMatchCondition(
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

  public static ListValue convertToListValue(List<String> values) {
    ListValue.Builder listValue = ListValue.newBuilder();
    values.forEach(value -> listValue.addValues(Value.newBuilder().setStringValue(value)));
    return listValue.build();
  }
}
