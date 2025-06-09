package ai.traceable.customsignature.config.service.rules.converter.expression;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_INT;
import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.edge.decision.converter.utils.Constants.ATTRIBUTE_NAME_LHS;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.Clause.ClauseCase;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import java.util.Set;

public class MatchExpressionConverter implements CustomSignatureExpressionConverter {
  private static final Set<MatchOperator> INT_MATCH_OPERATORS =
      Set.of(MatchOperator.MATCH_OPERATOR_GREATER_THAN, MatchOperator.MATCH_OPERATOR_LESS_THAN);

  private static final Set<MatchOperator> REGEX_MATCH_OPERATORS =
      Set.of(
          MatchOperator.MATCH_OPERATOR_MATCHES_REGEX, MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX);

  private static final Set<MatchOperator> ALL_MATCH_OPERATORS =
      Set.of(
          MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
          MatchOperator.MATCH_OPERATOR_NOT_CONTAIN,
          MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX,
          MatchOperator.MATCH_OPERATOR_GREATER_THAN,
          MatchOperator.MATCH_OPERATOR_LESS_THAN);

  private static final Set<MatchKey> MATCH_KEY_TYPES_WITH_ONLY_VALUE =
      Set.of(
          MatchKey.MATCH_KEY_URL,
          MatchKey.MATCH_KEY_HOST,
          MatchKey.MATCH_KEY_HTTP_METHOD,
          MatchKey.MATCH_KEY_USER_AGENT,
          MatchKey.MATCH_KEY_STATUS_CODE,
          MatchKey.MATCH_KEY_BODY,
          MatchKey.MATCH_KEY_BODY_SIZE,
          MatchKey.MATCH_KEY_QUERY_PARAMS_COUNT,
          MatchKey.MATCH_KEY_HEADERS_COUNT,
          MatchKey.MATCH_KEY_COOKIES_COUNT);

  private static final Set<MatchKey> MATCH_KEY_TYPES_WITH_KEY_EXPRESSION =
      Set.of(
          MatchKey.MATCH_KEY_HEADER_NAME,
          MatchKey.MATCH_KEY_PARAMETER_NAME,
          MatchKey.MATCH_KEY_QUERY_PARAMETER_NAME,
          MatchKey.MATCH_KEY_BODY_PARAMETER_NAME,
          MatchKey.MATCH_KEY_COOKIE_NAME);

  @Override
  public MatchCondition buildMatchCondition(Clause clause) {
    MatchExpression matchExpression = clause.getMatchExpression();
    if (!matchExpression.getMatchCategory().equals(MatchCategory.MATCH_CATEGORY_REQUEST)) {
      throw new IllegalArgumentException(
          "Invalid match category: " + matchExpression.getMatchCategory());
    }

    MatchKey matchKey = matchExpression.getMatchKey();
    MatchOperator matchOperator = matchExpression.getMatchOperator();
    String matchValue =
        matchExpression.hasValue()
            ? matchExpression.getValue().getStringValue()
            : matchExpression.getMatchValue();

    if (MATCH_KEY_TYPES_WITH_ONLY_VALUE.contains(matchKey)) {
      return buildStructuredMatchCondition(matchKey, matchOperator, matchValue);
    }

    if (MATCH_KEY_TYPES_WITH_KEY_EXPRESSION.contains(matchKey)) {
      // MatchKey types that are parsed as Map<String, String> or
      // Map<String, List<String>> in the edge-decision-service
      // && the expression/condition is on the keySet.
      return buildKeyExpressionMatchCondition(matchKey, matchOperator, matchValue);
    }

    // MatchKey types that are parsed as Map<String, String> or
    // Map<String, List<String>> in the edge-decision-service
    // && the expression/condition is on the valueSet.
    if (matchKey.equals(MatchKey.MATCH_KEY_PARAMETER_VALUE)) {
      return buildParameterValueMatchCondition(matchOperator, matchValue);
    }
    return buildGenericMatchCondition(matchKey, matchOperator, matchValue);
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return ClauseCase.MATCH_EXPRESSION;
  }

  private MatchCondition buildStructuredMatchCondition(
      MatchKey matchKey, MatchOperator matchOperator, String matchValue) {
    boolean isMatchKeyCaseInsensitive =
        CustomSignatureExpressionConverterUtils.isMatchKeyCaseInsensitive(matchKey);
    BinaryOperator.Builder builder =
        BinaryOperator.newBuilder()
            .setMatchOperator(
                CustomSignatureExpressionConverterUtils.getDataTransformationOperator(
                    isMatchKeyCaseInsensitive, matchOperator));
    FieldType fieldType = FIELD_TYPE_STR;

    if (INT_MATCH_OPERATORS.contains(matchOperator)) {
      builder.setNumberValue(Double.parseDouble(matchValue));
      fieldType = FIELD_TYPE_INT;
    } else if (REGEX_MATCH_OPERATORS.contains(matchOperator)) {
      builder.setRegex(matchValue);
    } else {
      builder.setStringValue(matchValue);
    }

    StructuredMatchCondition structuredMatchCondition =
        StructuredMatchCondition.newBuilder()
            .setLhs(
                AttributeDerivationMapping.newBuilder()
                    .setName(ATTRIBUTE_NAME_LHS)
                    .setType(fieldType)
                    .addRules(
                        DerivationRule.newBuilder()
                            .setTransformationConfig(
                                DataTransformationConfig.newBuilder()
                                    .setOutputType(fieldType)
                                    .setJexlExpression(
                                        JexlExpressionConfig.newBuilder()
                                            .setJexlExpression(
                                                CustomSignatureExpressionConverterUtils
                                                    .getJexlExpForMatchKey(matchKey))))))
            .setBinaryOperator(builder)
            .build();

    MatchCondition.Builder matchConditionBuilder =
        MatchCondition.newBuilder().setStructuredMatchCondition(structuredMatchCondition);
    // no first-class support of not contains currently
    if (matchOperator.equals(MatchOperator.MATCH_OPERATOR_NOT_CONTAIN)) {
      matchConditionBuilder.setNegate(true);
    }
    return matchConditionBuilder.build();
  }

  private MatchCondition buildKeyExpressionMatchCondition(
      MatchKey matchKey, MatchOperator matchOperator, String matchValue) {
    if (matchKey.equals(MatchKey.MATCH_KEY_PARAMETER_NAME)) {
      String jexlExpForQueryParams =
          String.format(
              "map:match(%s, %s, %s)",
              CustomSignatureExpressionConverterUtils.getJexlExpForMatchKey(
                  MatchKey.MATCH_KEY_QUERY_PARAMETER_NAME),
              CustomSignatureExpressionConverterUtils.getPredicateJexlExp(
                  matchOperator, matchValue),
              ALL_MATCH_OPERATORS.contains(matchOperator));

      String jexlExpForBodyParams =
          String.format(
              "map:match(%s, %s, %s)",
              CustomSignatureExpressionConverterUtils.getJexlExpForMatchKey(
                  MatchKey.MATCH_KEY_BODY_PARAMETER_NAME),
              CustomSignatureExpressionConverterUtils.getPredicateJexlExp(
                  matchOperator, matchValue),
              ALL_MATCH_OPERATORS.contains(matchOperator));

      return MatchCondition.newBuilder()
          .setLogicalMatchCondition(
              LogicalMatchCondition.newBuilder()
                  .setOperator(
                      ALL_MATCH_OPERATORS.contains(matchOperator)
                          ? LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND
                          : LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR)
                  .addConditions(
                      MatchCondition.newBuilder()
                          .setGenericMatchCondition(
                              GenericMatchCondition.newBuilder()
                                  .setJexlExpression(
                                      JexlExpressionConfig.newBuilder()
                                          .setJexlExpression(jexlExpForQueryParams))))
                  .addConditions(
                      MatchCondition.newBuilder()
                          .setGenericMatchCondition(
                              GenericMatchCondition.newBuilder()
                                  .setJexlExpression(
                                      JexlExpressionConfig.newBuilder()
                                          .setJexlExpression(jexlExpForBodyParams))))
                  .build())
          .build();
    }

    String jexlExp =
        String.format(
            "map:match(%s, %s, %s)",
            CustomSignatureExpressionConverterUtils.getJexlExpForMatchKey(matchKey),
            CustomSignatureExpressionConverterUtils.getPredicateJexlExp(matchOperator, matchValue),
            ALL_MATCH_OPERATORS.contains(matchOperator));

    return MatchCondition.newBuilder()
        .setGenericMatchCondition(
            GenericMatchCondition.newBuilder()
                .setJexlExpression(JexlExpressionConfig.newBuilder().setJexlExpression(jexlExp)))
        .build();
  }

  private MatchCondition buildParameterValueMatchCondition(
      MatchOperator matchOperator, String matchValue) {
    String jexlExpForQueryParams =
        String.format(
            "map:matchValue(%s, %s, %s)",
            getCollectValuesFromMapJexlExp(
                CustomSignatureExpressionConverterUtils.getJexlExpForMatchKey(
                    MatchKey.MATCH_KEY_QUERY_PARAMETER_VALUE)),
            CustomSignatureExpressionConverterUtils.getPredicateJexlExp(matchOperator, matchValue),
            ALL_MATCH_OPERATORS.contains(matchOperator));

    String jexlExpForBodyParams =
        String.format(
            "map:matchValue(%s, %s, %s)",
            getCollectValuesFromMapJexlExp(
                CustomSignatureExpressionConverterUtils.getJexlExpForMatchKey(
                    MatchKey.MATCH_KEY_BODY_PARAMETER_VALUE)),
            CustomSignatureExpressionConverterUtils.getPredicateJexlExp(matchOperator, matchValue),
            ALL_MATCH_OPERATORS.contains(matchOperator));

    return MatchCondition.newBuilder()
        .setLogicalMatchCondition(
            LogicalMatchCondition.newBuilder()
                .setOperator(
                    ALL_MATCH_OPERATORS.contains(matchOperator)
                        ? LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND
                        : LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR)
                .addConditions(
                    MatchCondition.newBuilder()
                        .setGenericMatchCondition(
                            GenericMatchCondition.newBuilder()
                                .setJexlExpression(
                                    JexlExpressionConfig.newBuilder()
                                        .setJexlExpression(jexlExpForQueryParams))))
                .addConditions(
                    MatchCondition.newBuilder()
                        .setGenericMatchCondition(
                            GenericMatchCondition.newBuilder()
                                .setJexlExpression(
                                    JexlExpressionConfig.newBuilder()
                                        .setJexlExpression(jexlExpForBodyParams))))
                .build())
        .build();
  }

  private MatchCondition buildGenericMatchCondition(
      MatchKey matchKey, MatchOperator matchOperator, String matchValue) {
    String jexlExp =
        String.format(
            "map:matchValue(%s, %s, %s)",
            getCollectValuesFromMapJexlExp(
                CustomSignatureExpressionConverterUtils.getJexlExpForMatchKey(matchKey)),
            CustomSignatureExpressionConverterUtils.getPredicateJexlExp(matchOperator, matchValue),
            ALL_MATCH_OPERATORS.contains(matchOperator));

    return MatchCondition.newBuilder()
        .setGenericMatchCondition(
            GenericMatchCondition.newBuilder()
                .setJexlExpression(JexlExpressionConfig.newBuilder().setJexlExpression(jexlExp)))
        .build();
  }

  private String getCollectValuesFromMapJexlExp(String mapJexlExp) {
    return "map:collectValues(" + mapJexlExp + ")";
  }
}
