package ai.traceable.region.config.service.rules;

import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.RegionRule;
import java.util.List;
import java.util.Optional;

public interface RulesManager {
  List<RegionRule> getRegionRules();

  Optional<RegionRule> createRegionRule(CreateRegionRuleRequest createRuleRequest);

  Optional<RegionRule> updateRegionRule(RegionRule regionRule);

  boolean deleteRegionRule(String id);
}
