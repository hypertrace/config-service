package ai.traceable.blocking.config.service.regions;

import ai.traceable.blocking.config.service.v1.RegionBlockingRules;

public interface RegionBlockingManager {
  RegionBlockingRules getEnabledBlockingRules(String requestHash);
}
