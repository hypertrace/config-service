package ai.traceable.detection.exclusion.config.service.v1.rules.modsec;

import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;

public class ExclusionModsecRulesSupportChecker {
  public static boolean isModsecConditionSupported(
      DetectionExclusionCondition detectionExclusionCondition) {
    return detectionExclusionCondition.hasScopeCondition()
        || detectionExclusionCondition.hasAttributeMatchCondition()
        || detectionExclusionCondition.hasIpLocationTypeCondition()
        || detectionExclusionCondition.hasEventCondition()
        || detectionExclusionCondition.hasAnomalousAttributeCondition()
        || detectionExclusionCondition.hasIpAddressCondition()
        || detectionExclusionCondition.hasRegionCondition();
  }

  public static boolean isModsecExclusionTargetSupported(ExclusionTarget exclusionTarget) {
    return exclusionTarget == ExclusionTarget.EXCLUSION_TARGET_ALLOW
        || exclusionTarget == ExclusionTarget.EXCLUSION_TARGET_BLOCK;
  }
}
