package ai.traceable.blocking.config.service.regions;

import ai.traceable.blocking.config.service.v1.RuleActionType;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ActionTypeConverter {
  Optional<RuleActionType> convert(RegionRuleActionType regionRuleActionType) {
    switch (regionRuleActionType) {
      case REGION_RULE_ACTION_TYPE_BLOCK:
        return Optional.of(RuleActionType.RULE_ACTION_TYPE_BLOCK);
      default:
        log.error("Unknown region rule action type {}", regionRuleActionType);
        return Optional.empty();
    }
  }
}
