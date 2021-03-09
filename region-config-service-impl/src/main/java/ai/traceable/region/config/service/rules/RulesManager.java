package ai.traceable.region.config.service.rules;

import ai.traceable.region.config.service.v1.RegionRule;
import java.util.List;

public interface RulesManager {
  List<RegionRule> getRegionRules();
}
