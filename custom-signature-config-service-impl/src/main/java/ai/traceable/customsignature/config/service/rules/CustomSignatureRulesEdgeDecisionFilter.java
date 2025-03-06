package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CustomSignatureRulesEdgeDecisionFilter {

  private CustomSignatureRulesEdgeDecisionFilter() {
    // utility classes shouldn't have public constructor
  }

  // filter out rules that will be evaluated by edge decision service
  // returns rules that should be evaluated by the platform
  public static List<CustomSignatureRule> getFilteredRules(List<CustomSignatureRule> rules) {
    return rules.stream()
        .map(CustomSignatureRulesEdgeDecisionFilter::getFilteredRule)
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  // Rules that can be converted to edge decision rules
  public static List<CustomSignatureRule> getConvertibleRules(List<CustomSignatureRule> rules) {
    return rules.stream()
        .filter(
            rule ->
                CustomSignatureRulesEdgeDecisionFilter.hasCompatibleClauseGroup(
                    rule.getDefinition().getClauseGroup()))
        .filter(
            rule -> CustomSignatureRulesEdgeDecisionFilter.hasCompatibleEventType(rule.getEffect()))
        .collect(Collectors.toUnmodifiableList());
  }

  private static Optional<CustomSignatureRule> getFilteredRule(CustomSignatureRule rule) {
    if (hasCompatibleClauseGroup(rule.getDefinition().getClauseGroup())
        && hasCompatibleEventType(rule.getEffect())) {
      return Optional.empty();
    }
    return Optional.of(rule);
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
      default:
        return true;
    }
  }

  public static boolean hasCompatibleEventType(RuleEffect ruleEffect) {
    return ruleEffect.getEventType().equals(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
        || ruleEffect.getEventType().equals(EventType.EVENT_TYPE_ALLOW)
        || (ruleEffect.getEventType().equals(EventType.EVENT_TYPE_NORMAL_DETECTION)
            && !ruleEffect.getEffectsList().isEmpty())
        || (ruleEffect.getEventType().equals(EventType.EVENT_TYPE_TESTING_DETECTION)
            && !ruleEffect.getEffectsList().isEmpty());
  }
}
