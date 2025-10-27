package ai.traceable.customsignature.config.service.rules.converter.expression;

import static ai.traceable.customsignature.config.service.rules.ClauseGroupValidator.KEY_NULL_MATCH_KEYS;

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
    ai.traceable.datamodel.data.transformation.config.v1.MatchOperator
        convertedLhsRhsMatchOperator =
            CustomSignatureExpressionConverterUtils.getDataTransformationOperator(
                isAnyMatchKeyCaseInsensitive, lhsRhsMatchOperator);

    if (KEY_NULL_MATCH_KEYS.contains(lhsMatchKey) && KEY_NULL_MATCH_KEYS.contains(rhsMatchKey)) {
      return String.format(
          "map:match(%s, %s, %s)",
          getValueForKeyNullMatchKeyTypes(lhsMatchKey),
          getValueForKeyNullMatchKeyTypes(rhsMatchKey),
          convertedLhsRhsMatchOperator.name());
    }

    if (KEY_NULL_MATCH_KEYS.contains(lhsMatchKey)) {
      String mapJexlExpression =
          CustomSignatureExpressionConverterUtils.getJexlExpressionForLhsRhsMatchKey(rhsMatchKey);
      String rhsKeyExpressionMatchValue =
          rhsKeyExpression.hasValue()
              ? rhsKeyExpression.getValue().getStringValue()
              : rhsKeyExpression.getMatchValue();
      String predicateJexlExpression =
          CustomSignatureExpressionConverterUtils.getPredicateJexlExpression(
              rhsKeyExpression.getMatchOperator(), rhsKeyExpressionMatchValue);
      return String.format(
          "map:match(%s, %s, %s)",
          getValueForKeyNullMatchKeyTypes(lhsMatchKey),
          getExtractedValuesForKeyValueMatchKeyTypes(mapJexlExpression, predicateJexlExpression),
          convertedLhsRhsMatchOperator.name());
    }

    if (KEY_NULL_MATCH_KEYS.contains(rhsMatchKey)) {
      String mapJexlExpression =
          CustomSignatureExpressionConverterUtils.getJexlExpressionForLhsRhsMatchKey(lhsMatchKey);
      String lhsKeyExpressionMatchValue =
          lhsKeyExpression.hasValue()
              ? lhsKeyExpression.getValue().getStringValue()
              : lhsKeyExpression.getMatchValue();
      String predicateJexlExpression =
          CustomSignatureExpressionConverterUtils.getPredicateJexlExpression(
              lhsKeyExpression.getMatchOperator(), lhsKeyExpressionMatchValue);
      return String.format(
          "map:match(%s, %s, %s)",
          getExtractedValuesForKeyValueMatchKeyTypes(mapJexlExpression, predicateJexlExpression),
          getValueForKeyNullMatchKeyTypes(rhsMatchKey),
          convertedLhsRhsMatchOperator.name());
    }

    String lhsMapJexlExpression =
        CustomSignatureExpressionConverterUtils.getJexlExpressionForLhsRhsMatchKey(lhsMatchKey);
    String rhsMapJexlExpression =
        CustomSignatureExpressionConverterUtils.getJexlExpressionForLhsRhsMatchKey(rhsMatchKey);
    String lhsKeyExpressionMatchValue =
        lhsKeyExpression.hasValue()
            ? lhsKeyExpression.getValue().getStringValue()
            : lhsKeyExpression.getMatchValue();
    String rhsKeyExpressionMatchValue =
        rhsKeyExpression.hasValue()
            ? rhsKeyExpression.getValue().getStringValue()
            : rhsKeyExpression.getMatchValue();
    String lhsPredicateJexlExpression =
        CustomSignatureExpressionConverterUtils.getPredicateJexlExpression(
            lhsKeyExpression.getMatchOperator(), lhsKeyExpressionMatchValue);
    String rhsPredicateJexlExpression =
        CustomSignatureExpressionConverterUtils.getPredicateJexlExpression(
            rhsKeyExpression.getMatchOperator(), rhsKeyExpressionMatchValue);

    return String.format(
        "map:match(%s, %s, %s)",
        getExtractedValuesForKeyValueMatchKeyTypes(
            lhsMapJexlExpression, lhsPredicateJexlExpression),
        getExtractedValuesForKeyValueMatchKeyTypes(
            rhsMapJexlExpression, rhsPredicateJexlExpression),
        convertedLhsRhsMatchOperator.name());
  }

  private String getExtractedValuesForKeyValueMatchKeyTypes(
      String mapJexlExpression, String predicateJexlExpression) {
    return "map:extractValues(" + mapJexlExpression + ", " + predicateJexlExpression + ")";
  }

  private String getValueForKeyNullMatchKeyTypes(MatchKey matchKey) {
    return "Set.of("
        + CustomSignatureExpressionConverterUtils.getJexlExpressionForMatchKey(matchKey)
        + ")";
  }
}
