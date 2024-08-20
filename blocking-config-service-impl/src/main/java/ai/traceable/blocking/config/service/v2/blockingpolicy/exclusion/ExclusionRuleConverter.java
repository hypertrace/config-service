package ai.traceable.blocking.config.service.v2.blockingpolicy.exclusion;

import ai.traceable.blocking.config.service.v2.ExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionModsecRule;

public interface ExclusionRuleConverter {
  ExclusionRule convert(DetectionExclusionModsecRule detectionExclusionModsecRule);
}
