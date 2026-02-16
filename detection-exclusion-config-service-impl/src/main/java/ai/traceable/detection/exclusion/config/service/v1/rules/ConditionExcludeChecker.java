package ai.traceable.detection.exclusion.config.service.v1.rules;

import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALLOW;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_BLOCK;

import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import java.util.List;

/**
 * Utility class to check if conditions have exclude enabled. This is used for validation and
 * runtime filtering since libtraceable doesn't support exclude for ALLOW/BLOCK exclusion targets
 * yet.
 *
 * <p>TODO: Remove this utility class when support for exclude gets added in libtraceable.
 */
public final class ConditionExcludeChecker {

  private ConditionExcludeChecker() {}

  /**
   * Checks if a rule has ALLOW or BLOCK exclusion target and contains any condition with exclude
   * enabled.
   */
  public static boolean hasExcludeEnabledForAllowOrBlockTarget(DetectionExclusionRule rule) {
    if (!rule.hasRuleInfo()) {
      return false;
    }
    List<ExclusionTarget> exclusionTargets = rule.getRuleInfo().getExclusionTargetsList();
    boolean hasAllowOrBlockTarget =
        exclusionTargets.contains(EXCLUSION_TARGET_ALLOW)
            || exclusionTargets.contains(EXCLUSION_TARGET_BLOCK);
    if (!hasAllowOrBlockTarget) {
      return false;
    }
    return hasAnyConditionWithExcludeEnabled(rule.getRuleInfo().getConditionsList());
  }

  /** Checks if any condition in the list has exclude enabled. */
  public static boolean hasAnyConditionWithExcludeEnabled(
      List<DetectionExclusionCondition> conditions) {
    return conditions.stream().anyMatch(ConditionExcludeChecker::hasExcludeEnabled);
  }

  /** Checks if a single condition has exclude enabled. */
  public static boolean hasExcludeEnabled(DetectionExclusionCondition condition) {
    switch (condition.getConditionCase()) {
      case IP_ADDRESS_CONDITION:
        return condition.getIpAddressCondition().getExclude();
      case REGION_CONDITION:
        return condition.getRegionCondition().getExclude();
      case IP_LOCATION_TYPE_CONDITION:
        return condition.getIpLocationTypeCondition().getExclude();
      case SCOPE_CONDITION:
        return condition.getScopeCondition().getExclude();
      default:
        return false;
    }
  }
}
