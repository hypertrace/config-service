package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision;

import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;

public class ExclusionEdgeDecisionRulesSupportChecker {
  public static boolean isEdgeDecisionConditionSupported(
      DetectionExclusionCondition detectionExclusionCondition) {
    return detectionExclusionCondition.hasAttributeMatchCondition()
        || detectionExclusionCondition.hasIpAddressCondition()
        || detectionExclusionCondition.hasIpLocationTypeCondition()
        || detectionExclusionCondition.hasLhsRhsKeysCondition()
        || detectionExclusionCondition.hasScopeCondition()
        || detectionExclusionCondition.hasConditionalExpression()
        || detectionExclusionCondition.hasRegionCondition()
        || detectionExclusionCondition.hasEventCondition();
  }

  public static boolean isEdgeDecisionTargetSupported(ExclusionTarget exclusionTarget) {
    return exclusionTarget == ExclusionTarget.EXCLUSION_TARGET_ALLOW
        || exclusionTarget == ExclusionTarget.EXCLUSION_TARGET_BLOCK;
  }
}
