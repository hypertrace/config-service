package ai.traceable.ratelimiting.service.v2.rules.shared;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleRecord;
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
                action.hasBlock()
                    || action.getMarkForTesting().hasAgentRuleEffect()
                    || action.getAlert().hasAgentRuleEffect())
        .findAny();
  }

  // filter out rule records that will be evaluated by edge decision service
  // this call will return rule records that should be evaluated by the platform
  public static List<RateLimitingRuleRecord> getFilteredRuleRecords(
      List<RateLimitingRuleRecord> ruleRecords, boolean removeAllEdgeCompatibleBlockRules) {
    return ruleRecords.stream()
        .map(ruleRecord -> getFilteredRuleRecord(ruleRecord, removeAllEdgeCompatibleBlockRules))
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private static Optional<RateLimitingRuleRecord> getFilteredRuleRecord(
      RateLimitingRuleRecord ruleRecord, boolean removeAllEdgeCompatibleBlockRules) {
    return getFilteredRule(ruleRecord.getRule(), removeAllEdgeCompatibleBlockRules)
        .map(filteredRule -> ruleRecord.toBuilder().setRule(filteredRule).build());
  }

  // Rules that can be converted to edge decision rules
  public static List<RateLimitingRule> getConvertibleRules(List<RateLimitingRule> rules) {
    return rules.stream()
        .filter(rule -> RateLimitingRulesEdgeDecisionFilter.isCategorySupported(rule.getData()))
        .map(RateLimitingRulesEdgeDecisionFilter::getConvertibleRule)
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private static Optional<RateLimitingRule> getFilteredRule(
      RateLimitingRule rule, boolean removeAllEdgeCompatibleBlockRules) {
    // check if this category is supported by edge decision rules
    if (isCategorySupported(rule.getData())
        && RateLimitingRulesEdgeDecisionValidator.isCompatibleCondition(
            rule.getData().getCondition())) {
      List<ThresholdActionConfig> filteredThresholdActionConfigs =
          rule.getData().getThresholdActionConfigsList().stream()
              .filter(
                  thresholdActionConfig ->
                      doesNotContainMatchingEdgeDecisionAction(
                          thresholdActionConfig, removeAllEdgeCompatibleBlockRules))
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
    }
    return Optional.of(rule);
  }

  public static boolean meetsEdgeDecisionRequirements(RateLimitingRuleData rateLimitingRuleData) {
    List<ThresholdActionConfig> filteredThresholdActionConfigs =
        rateLimitingRuleData.getThresholdActionConfigsList().stream()
            .filter(
                thresholdActionConfig ->
                    RateLimitingRulesEdgeDecisionFilter.findAnyMatchingEdgeDecisionAction(
                            thresholdActionConfig)
                        .isPresent())
            .collect(Collectors.toUnmodifiableList());
    return !filteredThresholdActionConfigs.isEmpty();
  }

  private static boolean isCategorySupported(RateLimitingRuleData rateLimitingRuleData) {
    Category category = rateLimitingRuleData.getCategory();
    return category.equals(Category.CATEGORY_RATE_LIMITING)
        || category.equals(Category.CATEGORY_ENUMERATION);
  }

  /**
   * Checks if threshold actions contain no actions that should be processed at edge.
   * Edge-compatible actions include Block-with-duration actions, i.e., Any block action (when
   * removeAllEdgeCompatibleBlockRules = true), Mark-for-testing/Alert actions with agent rule
   * effects. Returns true only when NO edge-compatible actions are present.
   */
  private static boolean doesNotContainMatchingEdgeDecisionAction(
      ThresholdActionConfig thresholdActionConfig, boolean removeAllEdgeCompatibleBlockRules) {
    return thresholdActionConfig.getActionsList().stream()
        .noneMatch(
            action ->
                action.getBlock().getUseThresholdDuration()
                    || (action.hasBlock() && removeAllEdgeCompatibleBlockRules)
                    || action.getMarkForTesting().hasAgentRuleEffect()
                    || action.getAlert().hasAgentRuleEffect());
  }

  private static Optional<RateLimitingRule> getConvertibleRule(RateLimitingRule rule) {
    if (!RateLimitingRulesEdgeDecisionValidator.isCompatibleCondition(
        rule.getData().getCondition())) {
      return Optional.empty();
    }
    List<ThresholdActionConfig> filteredThresholdActionConfigs =
        rule.getData().getThresholdActionConfigsList().stream()
            .filter(
                thresholdActionConfig ->
                    RateLimitingRulesEdgeDecisionFilter.findAnyMatchingEdgeDecisionAction(
                            thresholdActionConfig)
                        .isPresent())
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
}
