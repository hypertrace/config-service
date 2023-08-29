package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.Region;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RegionDataHandler extends MaliciousSourceDataHandler {
  @Inject
  public RegionDataHandler(BlockingRulesUtils blockingRulesUtils) {
    super(blockingRulesUtils);
  }

  @Override
  protected Category getCategory() {
    return Category.CUSTOM_REGION_RULE;
  }

  @Override
  protected BlockingDetails generateBlockingDetails(MaliciousSourcesRule rule) {
    return RegionBlockingDetails.builder()
        .regions(
            rule.getRuleInfo().getConditionsList().stream()
                .flatMap(
                    condition ->
                        condition.getRegionCondition().getRegionsList().stream()
                            .map(Region::getCountryIsoCode))
                .distinct()
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }

  @Override
  protected Optional<BlockingPolicyDataBucket> getRuleBucket(String id, RuleActionType actionType) {
    switch (actionType) {
      case RULE_ACTION_TYPE_BLOCK:
        return Optional.of(BlockingPolicyDataBucket.REGION_VIOLATIONS);
      case RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT:
        return Optional.of(BlockingPolicyDataBucket.REGION_BLOCK_ALL_EXCEPT_VIOLATIONS);
      case RULE_ACTION_TYPE_ALERT:
        return Optional.empty();
      default:
        log.error("Invalid rule action type: {} for rule with rule id: {}", actionType, id);
        return Optional.empty();
    }
  }
}
