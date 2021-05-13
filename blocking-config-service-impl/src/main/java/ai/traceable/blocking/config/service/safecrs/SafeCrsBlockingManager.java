package ai.traceable.blocking.config.service.safecrs;

import ai.traceable.blocking.config.service.v1.SafeCrsBlockingRules;

public interface SafeCrsBlockingManager {

  SafeCrsBlockingRules getBlockingRules(String requestHash);
}
