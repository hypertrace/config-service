package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import java.util.List;
import java.util.stream.Collectors;

public class CustomSignatureRulesEdgeDecisionFilter {

  private CustomSignatureRulesEdgeDecisionFilter() {
    // utility classes shouldn't have a public constructor
  }

  // Rules that can be converted to edge decision rules
  public static List<CustomSignatureRule> getConvertibleRules(List<CustomSignatureRule> rules) {
    return rules.stream()
        .filter(rule -> hasCompatibleClauseGroup(rule.getDefinition().getClauseGroup()))
        .filter(rule -> hasCompatibleEventType(rule.getEffect()))
        .collect(Collectors.toUnmodifiableList());
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
        return clause
                .getLhsRhsKeysExpression()
                .getLhsKeyExpression()
                .getMatchCategory()
                .equals(MatchCategory.MATCH_CATEGORY_REQUEST)
            && clause
                .getLhsRhsKeysExpression()
                .getRhsKeyExpression()
                .getMatchCategory()
                .equals(MatchCategory.MATCH_CATEGORY_REQUEST);
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
}
