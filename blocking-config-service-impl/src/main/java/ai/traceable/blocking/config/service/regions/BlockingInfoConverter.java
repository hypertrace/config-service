package ai.traceable.blocking.config.service.regions;

import ai.traceable.blocking.config.service.v1.BlockingInfo;
import ai.traceable.platform.opa.v1.BlockingCategory;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.region.config.service.v1.RegionRule;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BlockingInfoConverter {
  Optional<BlockingInfo> convert(RegionRule regionRule) {
    return this.buildInfo(regionRule)
        .map(
            info ->
                BlockingInfo.newBuilder()
                    .setInfo(info)
                    .setCategory(BlockingCategory.CUSTOM_REGION_RULE.name())
                    .setExpirationMillis(regionRule.getExpirationMillis())
                    .build());
  }

  private Optional<String> buildInfo(RegionRule rule) {
    switch (rule.getActionType()) {
      case REGION_RULE_ACTION_TYPE_BLOCK:
        return Optional.of(
            ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo(
                rule.getId(), rule.getName()));
      default:
        log.error(
            "Unknown region rule action type {} for region rule {}", rule.getActionType(), rule);
        return Optional.empty();
    }
  }
}
