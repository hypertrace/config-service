package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition;

import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadata;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadataMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.LhsRhsKeysCondition;
import ai.traceable.detection.exclusion.config.service.v1.MatchOperator;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DetectionExclusionRuleLhsRhsKeysConditionConverter
    implements DetectionExclusionRuleConditionConverter {
  @Override
  public MatchCondition buildMatchCondition(
      RequestContext requestContext, DetectionExclusionCondition condition) {
    LhsRhsKeysCondition lhsRhsKeysCondition = condition.getLhsRhsKeysCondition();
    KeyMetadataMatchCondition lhsKeyMetadataMatchCondition =
        lhsRhsKeysCondition.getLhsKeyCondition();
    if (DetectionExclusionRuleConditionConverterUtils.RESPONSE_KEY_METADATA.contains(
        lhsKeyMetadataMatchCondition.getMetadata())) {
      throw new IllegalArgumentException(
          "Illegal lhs key condition metadata: " + lhsKeyMetadataMatchCondition.getMetadata());
    }
    KeyMetadataMatchCondition rhsKeyMetadataMatchCondition =
        lhsRhsKeysCondition.getRhsKeyCondition();
    if (DetectionExclusionRuleConditionConverterUtils.RESPONSE_KEY_METADATA.contains(
        lhsKeyMetadataMatchCondition.getMetadata())) {
      throw new IllegalArgumentException(
          "Illegal rhs key condition metadata: " + rhsKeyMetadataMatchCondition.getMetadata());
    }
    return MatchCondition.newBuilder()
        .setGenericMatchCondition(
            GenericMatchCondition.newBuilder()
                .setJexlExpression(
                    JexlExpressionConfig.newBuilder()
                        .setJexlExpression(
                            buildJexlExpressionForLhsAndRhsKeyMetadataMatchConditions(
                                lhsKeyMetadataMatchCondition,
                                rhsKeyMetadataMatchCondition,
                                lhsRhsKeysCondition.getLhsRhsMatchOperator()))))
        .build();
  }

  @Override
  public DetectionExclusionCondition.ConditionCase getConditionCase() {
    return DetectionExclusionCondition.ConditionCase.LHS_RHS_KEYS_CONDITION;
  }

  private String buildJexlExpressionForLhsAndRhsKeyMetadataMatchConditions(
      KeyMetadataMatchCondition lhsKeyMetadataMatchCondition,
      KeyMetadataMatchCondition rhsKeyMetadataMatchCondition,
      MatchOperator lhsRhsMatchOperator) {
    KeyMetadata lhsKeyMetadata = lhsKeyMetadataMatchCondition.getMetadata();
    KeyMetadata rhsKeyMetadata = rhsKeyMetadataMatchCondition.getMetadata();

    /*
     * This should be false in most cases, but we maintain this check:-
     * In case any new KeyMetadata types are added in the future that might be case-insensitive
     * OR
     * If the LhsRhsKeysMatchCondition logic is modified to support match condition evaluation with case-insensitive key metadata types
     */
    boolean isLhsOrRhsKeyMetadataCaseInsensitive =
        DetectionExclusionRuleConditionConverterUtils.isKeyMetadataCaseInsensitive(lhsKeyMetadata)
            || DetectionExclusionRuleConditionConverterUtils.isKeyMetadataCaseInsensitive(
                rhsKeyMetadata);

    return String.format(
        "map:match(%s, %s, %s, %s, %s)",
        DetectionExclusionRuleConditionConverterUtils.getJexlExpForLhsRhsMatchConditionKeyMetadata(
            lhsKeyMetadata),
        DetectionExclusionRuleConditionConverterUtils.getJexlExpForLhsRhsMatchConditionKeyMetadata(
            rhsKeyMetadata),
        DetectionExclusionRuleConditionConverterUtils.getPredicateJexlExp(
            lhsKeyMetadataMatchCondition.getMatchCondition()),
        DetectionExclusionRuleConditionConverterUtils.getPredicateJexlExp(
            rhsKeyMetadataMatchCondition.getMatchCondition()),
        DetectionExclusionRuleConditionConverterUtils.getMatchOperator(
            isLhsOrRhsKeyMetadataCaseInsensitive, lhsRhsMatchOperator));
  }
}
