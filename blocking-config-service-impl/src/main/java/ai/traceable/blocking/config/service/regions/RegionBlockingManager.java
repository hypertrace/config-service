package ai.traceable.blocking.config.service.regions;

import ai.traceable.blocking.config.service.v1.BlockingRule;
import java.util.List;

public interface RegionBlockingManager {
  List<BlockingRule> getBlockingRules();
}
