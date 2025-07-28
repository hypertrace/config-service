package ai.traceable.customsignature.config.service.rules.converter.expression;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.LhsRhsKeysExpression;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;

public class LhsRhsKeysExpressionConverter implements CustomSignatureExpressionConverter {

  @Override
  public MatchCondition buildMatchCondition(Clause clause) {
    LhsRhsKeysExpression lhsRhsKeysExpression = clause.getLhsRhsKeysExpression();
    /*
     * Both MATCH_OPERATOR_CONTAINS and MATCH_OPERATOR_NOT_CONTAIN map to the same data transformation
     * MatchOperator.MATCH_OPERATOR_CONTAINS
     * (as can be seen in CustomSignatureExpressionConverterUtils.getDataTransformationOperator).
     *
     * The 'negationValue' boolean is used to differentiate between these operators by controlling
     * the 'negate' property in the resulting MatchCondition.
     */
    boolean negationValue =
        lhsRhsKeysExpression.getMatchOperator() == MatchOperator.MATCH_OPERATOR_NOT_CONTAIN;

    MatchExpression lhsMatchExpression =
        lhsRhsKeysExpression.hasLhsKeyExpression()
            ? lhsRhsKeysExpression.getLhsKeyExpression()
            : lhsRhsKeysExpression.getKeyLhsExpression();
    MatchExpression rhsMatchExpression =
        lhsRhsKeysExpression.hasRhsKeyExpression()
            ? lhsRhsKeysExpression.getRhsKeyExpression()
            : lhsRhsKeysExpression.getKeyRhsExpression();

    return MatchCondition.newBuilder()
        .setGenericMatchCondition(
            GenericMatchCondition.newBuilder()
                .setJexlExpression(
                    JexlExpressionConfig.newBuilder()
                        .setJexlExpression(
                            buildJexlExpressionForLhsAndRhsKeyMatchExpressions(
                                lhsMatchExpression,
                                rhsMatchExpression,
                                lhsRhsKeysExpression.getMatchOperator()))
                        .build())
                .build())
        .setNegate(negationValue)
        .build();
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.LHS_RHS_KEYS_EXPRESSION;
  }

  private String buildJexlExpressionForLhsAndRhsKeyMatchExpressions(
      MatchExpression lhsKeyExpression,
      MatchExpression rhsKeyExpression,
      MatchOperator lhsRhsMatchOperator) {
    String lhsKeyExpressionMatchValue =
        lhsKeyExpression.hasValue()
            ? lhsKeyExpression.getValue().getStringValue()
            : lhsKeyExpression.getMatchValue();
    String rhsKeyExpressionMatchValue =
        rhsKeyExpression.hasValue()
            ? rhsKeyExpression.getValue().getStringValue()
            : rhsKeyExpression.getMatchValue();
    MatchKey lhsMatchKey = lhsKeyExpression.getMatchKey();
    MatchKey rhsMatchKey = rhsKeyExpression.getMatchKey();

    /*
     * This should be false in most cases, but we maintain this check:-
     * In case any new MatchKey types are added in the future that might be case-insensitive
     * OR
     * If the LhsRhsKeysExpression logic is modified to support match expression evaluation with case-insensitive match keys
     */
    boolean isAnyMatchKeyCaseInsensitive =
        CustomSignatureExpressionConverterUtils.isMatchKeyCaseInsensitive(lhsMatchKey)
            || CustomSignatureExpressionConverterUtils.isMatchKeyCaseInsensitive(rhsMatchKey);

    return String.format(
        "map:match(%s, %s, %s, %s, %s)",
        CustomSignatureExpressionConverterUtils.getJexlExpForLhsRhsMatchKey(lhsMatchKey),
        CustomSignatureExpressionConverterUtils.getJexlExpForLhsRhsMatchKey(rhsMatchKey),
        CustomSignatureExpressionConverterUtils.getPredicateJexlExp(
            lhsKeyExpression.getMatchOperator(), lhsKeyExpressionMatchValue),
        CustomSignatureExpressionConverterUtils.getPredicateJexlExp(
            rhsKeyExpression.getMatchOperator(), rhsKeyExpressionMatchValue),
        CustomSignatureExpressionConverterUtils.getDataTransformationOperator(
            isAnyMatchKeyCaseInsensitive, lhsRhsMatchOperator));
  }
}
