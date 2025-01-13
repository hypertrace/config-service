package ai.traceable.ratelimiting.service.v2.rules.shared;

import static ai.traceable.ratelimiting.config.service.v2.Action.ActionCase.BLOCK;
import static ai.traceable.ratelimiting.config.service.v2.Action.ActionCase.MARK_FOR_TESTING;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class RateLimitingRulesEdgeDecisionFilter {

  public static Optional<Action> findAnyMatchingEdgeDecisionAction(
      ThresholdActionConfig thresholdActionConfig) {
    return thresholdActionConfig.getActionsList().stream()
        .filter(
            action ->
                (action.getActionCase().equals(BLOCK)
                        && action.getBlock().getUseThresholdDuration())
                    || (action.getActionCase().equals(MARK_FOR_TESTING)
                        && action.getMarkForTesting().hasAgentRuleEffect()))
        .findAny();
  }

  public static List<RateLimitingRule> getFilteredRules(List<RateLimitingRule> rules) {
    return rules.stream()
        .map(RateLimitingRulesEdgeDecisionFilter::getFilteredRule)
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private static Optional<RateLimitingRule> getFilteredRule(RateLimitingRule rule) {
    List<ThresholdActionConfig> filteredThresholdActionConfigs =
        rule.getData().getThresholdActionConfigsList().stream()
            .filter(RateLimitingRulesEdgeDecisionFilter::doesNotContainMatchingEdgeDecisionAction)
            .collect(Collectors.toUnmodifiableList());
    if (filteredThresholdActionConfigs.isEmpty()) {
      return Optional.empty();
    } else if (filteredThresholdActionConfigs.size()
        < rule.getData().getThresholdActionConfigsList().size()) {
      RateLimitingRuleData.Builder builder =
          rule.getData().toBuilder()
              .clearThresholdActionConfigs()
              .addAllThresholdActionConfigs(filteredThresholdActionConfigs);
      return Optional.of(rule.toBuilder().setData(builder).build());
    }
    return Optional.of(rule);
  }

  private static boolean doesNotContainMatchingEdgeDecisionAction(
      ThresholdActionConfig thresholdActionConfig) {
    // if matching edge decision actions are empty, then return true
    return findAnyMatchingEdgeDecisionAction(thresholdActionConfig).isEmpty();
  }
}
