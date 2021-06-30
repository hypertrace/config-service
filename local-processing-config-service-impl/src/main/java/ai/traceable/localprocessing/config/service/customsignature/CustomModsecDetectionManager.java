package ai.traceable.localprocessing.config.service.customsignature;

import ai.traceable.localprocessing.config.service.v1.CustomModsecDetectionRules;

public interface CustomModsecDetectionManager {
  CustomModsecDetectionRules getEnabledRules(String requestHash);
}
