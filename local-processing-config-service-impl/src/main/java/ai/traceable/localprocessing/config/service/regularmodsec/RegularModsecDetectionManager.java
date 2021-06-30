package ai.traceable.localprocessing.config.service.regularmodsec;

import ai.traceable.localprocessing.config.service.v1.RegularModsecDetectionRules;

public interface RegularModsecDetectionManager {
  RegularModsecDetectionRules getDetectionRules(String requestHash);
}
