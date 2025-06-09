package ai.traceable.customsignature.config.service.rules.converter.expression;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.Clause.ClauseCase;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import java.util.Set;

public class KeyValueExpressionConverter implements CustomSignatureExpressionConverter {

  private static final Set<KeyValueTag> LIST_VALUE_MAP_TYPES =
      Set.of(KeyValueTag.KEY_VALUE_TAG_QUERY_PARAMETER, KeyValueTag.KEY_VALUE_TAG_BODY_PARAMETER);

  private static final Set<MatchOperator> ALL_MATCH_OPERATORS =
      Set.of(
          MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
          MatchOperator.MATCH_OPERATOR_NOT_CONTAIN,
          MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX,
          MatchOperator.MATCH_OPERATOR_GREATER_THAN,
          MatchOperator.MATCH_OPERATOR_LESS_THAN);

  @Override
  public MatchCondition buildMatchCondition(Clause clause) {
    KeyValueExpression keyValueExpression = clause.getKeyValueExpression();
    if (!keyValueExpression.getMatchCategory().equals(MatchCategory.MATCH_CATEGORY_REQUEST)) {
      throw new IllegalArgumentException(
          "Invalid match category: " + keyValueExpression.getMatchCategory());
    }

    if (keyValueExpression.getTag().equals(KeyValueTag.KEY_VALUE_TAG_PARAMETER)) {
      return buildParameterMatchCondition(keyValueExpression);
    }

    String jexlExp = buildJexlExpressionForTag(keyValueExpression, keyValueExpression.getTag());
    return MatchCondition.newBuilder()
        .setGenericMatchCondition(
            GenericMatchCondition.newBuilder()
                .setJexlExpression(JexlExpressionConfig.newBuilder().setJexlExpression(jexlExp)))
        .build();
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return ClauseCase.KEY_VALUE_EXPRESSION;
  }

  private MatchCondition buildParameterMatchCondition(KeyValueExpression keyValueExpression) {
    String jexlExpForQueryParams =
        buildJexlExpressionForTag(keyValueExpression, KeyValueTag.KEY_VALUE_TAG_QUERY_PARAMETER);
    String jexlExpForBodyParams =
        buildJexlExpressionForTag(keyValueExpression, KeyValueTag.KEY_VALUE_TAG_BODY_PARAMETER);

    return MatchCondition.newBuilder()
        .setLogicalMatchCondition(
            LogicalMatchCondition.newBuilder()
                .setOperator(getLogicalOperator(keyValueExpression))
                .addConditions(buildGenericMatchCondition(jexlExpForQueryParams))
                .addConditions(buildGenericMatchCondition(jexlExpForBodyParams))
                .build())
        .build();
  }

  private String buildJexlExpressionForTag(KeyValueExpression keyValueExpression, KeyValueTag tag) {
    return LIST_VALUE_MAP_TYPES.contains(tag)
        ? String.format(
            "map:match(%s, %s, %s, %s)",
            CustomSignatureExpressionConverterUtils.getJexlExpForTag(tag),
            CustomSignatureExpressionConverterUtils.getPredicateJexlExp(
                keyValueExpression.getKeyMatchOperator(), keyValueExpression.getMatchKey()),
            CustomSignatureExpressionConverterUtils.getPredicateJexlExp(
                keyValueExpression.getValueMatchOperator(), keyValueExpression.getMatchValue()),
            ALL_MATCH_OPERATORS.contains(keyValueExpression.getValueMatchOperator()))
        : String.format(
            "map:match(%s, %s, %s)",
            CustomSignatureExpressionConverterUtils.getJexlExpForTag(tag),
            CustomSignatureExpressionConverterUtils.getPredicateJexlExp(
                keyValueExpression.getKeyMatchOperator(), keyValueExpression.getMatchKey()),
            CustomSignatureExpressionConverterUtils.getPredicateJexlExp(
                keyValueExpression.getValueMatchOperator(), keyValueExpression.getMatchValue()));
  }

  private LogicalMatchOperator getLogicalOperator(KeyValueExpression keyValueExpression) {
    return ALL_MATCH_OPERATORS.contains(keyValueExpression.getValueMatchOperator())
        ? LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND
        : LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR;
  }

  private MatchCondition buildGenericMatchCondition(String jexlExp) {
    return MatchCondition.newBuilder()
        .setGenericMatchCondition(
            GenericMatchCondition.newBuilder()
                .setJexlExpression(JexlExpressionConfig.newBuilder().setJexlExpression(jexlExp)))
        .build();
  }
}
