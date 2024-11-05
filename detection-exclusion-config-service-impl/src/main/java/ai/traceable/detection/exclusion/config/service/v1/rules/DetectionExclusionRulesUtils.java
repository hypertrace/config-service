package ai.traceable.detection.exclusion.config.service.v1.rules;

import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import java.util.List;
import java.util.Optional;

public class DetectionExclusionRulesUtils {
  public static final String DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAMESPACE =
      "detectionExclusionRuleConfig";
  public static final String THRESHOLD_EXCEEDED_DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAME =
      "thresholdExceededDetectionExclusionRule";
  public static final String DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAME =
      "detectionExclusionRule";

  public static Optional<DetectionExclusionRule> filterConfig(
      DetectionExclusionRule detectionExclusionRule, GetRulesFilter filter) {
    return Optional.of(detectionExclusionRule)
        .filter(
            rule ->
                filter.getExclusionTargetsList().isEmpty()
                    || filter.getExclusionTargetsList().stream()
                        .anyMatch(
                            exclusionTarget ->
                                detectionExclusionRule
                                    .getRuleInfo()
                                    .getExclusionTargetsList()
                                    .contains(exclusionTarget)))
        .filter(
            rule ->
                filter.getRuleIdsList().isEmpty()
                    || filter.getRuleIdsList().contains(detectionExclusionRule.getId()))
        .filter(
            rule ->
                !filter.hasDisabled()
                    || filter.getDisabled()
                        == detectionExclusionRule.getRuleInfo().getRuleStatus().getDisabled())
        .filter(
            rule ->
                !filter.hasHidden()
                    || filter.getHidden()
                        == detectionExclusionRule.getRuleInfo().getRuleStatus().getHidden())
        .filter(
            rule ->
                filter.getRuleChangeSourcesList().isEmpty()
                    || filter
                        .getRuleChangeSourcesList()
                        .contains(rule.getRuleInfo().getRuleStatus().getChangeSource()))
        .filter(
            rule ->
                filter.getRuleCreationSourcesList().isEmpty()
                    || filter
                        .getRuleCreationSourcesList()
                        .contains(rule.getRuleInfo().getRuleStatus().getRuleCreationSource()))
        .filter(
            rule ->
                filter.getRuleIntentsList().isEmpty()
                    || filter
                        .getRuleIntentsList()
                        .contains(rule.getRuleInfo().getRuleStatus().getRuleIntent()))
        .filter(rule -> filterRuleOnScope(rule, filter.getRuleScope()));
  }

  /**
   * Method to filter on rule-scope * If filterScope has no environment scope, always return true *
   * If filterScope has environment scope but the environment scope has no environment IDs, return
   * true only if the rule has no Environment IDs in its rule-scope. * If filterScope has
   * environment scope and the environment scope has one or more environment IDs, return true only
   * if there is at least one overlap of environment ID between the filter and the rule.
   */
  private static boolean filterRuleOnScope(
      DetectionExclusionRule rule, DetectionExclusionRuleScope ruleScope) {
    List<String> ruleEnvironmentIds =
        rule.getRuleScope().getEnvironmentScope().getEnvironmentIdsList();
    if (!ruleScope.hasEnvironmentScope() || ruleEnvironmentIds.isEmpty()) {
      return true;
    }
    List<String> filterEnvironmentIds = ruleScope.getEnvironmentScope().getEnvironmentIdsList();
    return ruleEnvironmentIds.stream().anyMatch(filterEnvironmentIds::contains);
  }
}
