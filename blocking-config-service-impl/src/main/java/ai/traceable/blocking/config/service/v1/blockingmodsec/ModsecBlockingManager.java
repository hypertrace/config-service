package ai.traceable.blocking.config.service.v1.blockingmodsec;

import ai.traceable.blocking.config.service.v1.SafeCrsBlockingRules;

public interface ModsecBlockingManager {

  SafeCrsBlockingRules getBlockingRules(String requestHash);
}
