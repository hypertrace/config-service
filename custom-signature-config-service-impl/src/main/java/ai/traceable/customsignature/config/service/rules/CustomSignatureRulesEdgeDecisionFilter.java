package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.Category;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.LhsRhsKeysExpression;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CustomSignatureRulesEdgeDecisionFilter {

  // Rules that can be converted to edge decision rules AND are not expired
  public static List<CustomSignatureRule> getConvertibleAndActiveRules(
      List<CustomSignatureRule> rules) {
    return rules.stream()
        .filter(CustomSignatureRulesEdgeDecisionFilter::isEligibleForEdgeDecisionSupplier)
        .filter(rule -> !isExpiredRule(rule))
        .collect(Collectors.toUnmodifiableList());
  }

  private static boolean isEligibleForEdgeDecisionSupplier(CustomSignatureRule rule) {
    if (rule.getCategory() == Category.CATEGORY_AI_APP_PROTECTION) {
      return false;
    }
    return rule.getEffect()
        .getRuleEvaluationPointsList()
        .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE);
  }

  static boolean isExpiredRule(CustomSignatureRule rule) {
    if (!rule.hasBlockingExpiryDetails()) {
      return false;
    }
    return rule.getBlockingExpiryDetails().getExpiryTimestampMillis() <= System.currentTimeMillis();
  }

  public static boolean isConvertibleRule(RuleEffect ruleEffect, ClauseGroup clauseGroup) {
    return hasCompatibleEventType(ruleEffect) && hasCompatibleClauseGroup(clauseGroup);
  }

  private static boolean hasCompatibleClauseGroup(ClauseGroup clauseGroup) {
    return clauseGroup.getClausesList().stream()
        .allMatch(CustomSignatureRulesEdgeDecisionFilter::isCompatibleClause);
  }

  private static boolean isCompatibleClause(Clause clause) {
    switch (clause.getClauseCase()) {
      case ATTRIBUTE_KEY_VALUE_EXPRESSION:
      case CUSTOM_SEC_RULE:
      case REQUEST_SCANNER_TYPE_EXPRESSION:
        return false;
      case MATCH_EXPRESSION:
        return clause
            .getMatchExpression()
            .getMatchCategory()
            .equals(MatchCategory.MATCH_CATEGORY_REQUEST);
      case KEY_VALUE_EXPRESSION:
        return clause
            .getKeyValueExpression()
            .getMatchCategory()
            .equals(MatchCategory.MATCH_CATEGORY_REQUEST);
      case LHS_RHS_KEYS_EXPRESSION:
        return isCompatibleLhsRhsKeysExpression(clause.getLhsRhsKeysExpression());
      default:
        return true;
    }
  }

  private static boolean hasCompatibleEventType(RuleEffect ruleEffect) {
    return ruleEffect.getEventType().equals(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
        || ruleEffect.getEventType().equals(EventType.EVENT_TYPE_ALLOW)
        || (ruleEffect.getEventType().equals(EventType.EVENT_TYPE_NORMAL_DETECTION)
            && !ruleEffect.getEffectsList().isEmpty())
        || (ruleEffect.getEventType().equals(EventType.EVENT_TYPE_TESTING_DETECTION)
            && !ruleEffect.getEffectsList().isEmpty());
  }

  private static boolean isCompatibleLhsRhsKeysExpression(
      LhsRhsKeysExpression lhsRhsKeysExpression) {
    return areMatchCategoriesCompatible(
            lhsRhsKeysExpression.getLhsKeyExpression().getMatchCategory(),
            lhsRhsKeysExpression.getRhsKeyExpression().getMatchCategory())
        || areMatchCategoriesCompatible(
            lhsRhsKeysExpression.getKeyLhsExpression().getMatchCategory(),
            lhsRhsKeysExpression.getKeyRhsExpression().getMatchCategory());
  }

  private static boolean areMatchCategoriesCompatible(
      MatchCategory lhsMatchCategory, MatchCategory rhsMatchCategory) {
    return lhsMatchCategory == MatchCategory.MATCH_CATEGORY_REQUEST
        && rhsMatchCategory == MatchCategory.MATCH_CATEGORY_REQUEST;
  }
}
