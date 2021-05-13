package ai.traceable.blocking.config.service.customsignature;

import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;

public interface CustomModsecBlockingManager {

  CustomModsecBlockingRules getEnabledBlockingRules(String requestHash);
}
