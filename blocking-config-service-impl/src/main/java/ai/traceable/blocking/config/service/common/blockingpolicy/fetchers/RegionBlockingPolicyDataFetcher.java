package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.REGION_VIOLATIONS;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.region.config.service.v1.Country;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class RegionBlockingPolicyDataFetcher implements BlockingPolicyDataFetcherBase {

  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  RegionBlockingPolicyDataFetcher(BlockingRulesUtils blockingRulesUtils) {
    this.blockingRulesUtils = blockingRulesUtils;
  }

  @Override
  public BlockingPolicyAggregate<BlockingPolicyData> getBlockingPolicyData(
      RequestContext requestContext,
      BlockingPolicyDataFilter filter,
      BlockingRulesSupplier blockingRulesSupplier) {
    return new BlockingPolicyAggregate<>(
        blockingRulesSupplier.getRegionRules().stream()
            .map(this::getBlockingDetails)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toUnmodifiableList()));
  }

  private Optional<BlockingPolicyData> getBlockingDetails(RegionRule regionRule) {
    Optional<BlockingPolicyData> emptyOptionalOfBlockingPolicyData = Optional.empty();
    if (regionRule.getRegionIdList().isEmpty()
        || !blockingRulesUtils.isRuleActive(
            regionRule.getExpirationDetails().getTimestampMillis())) {
      return emptyOptionalOfBlockingPolicyData;
    }
    Optional<BlockingPolicyData.RuleType> ruleType =
        getRuleType(regionRule.getId(), regionRule.getActionType());
    Optional<BlockingPolicyDataBucket> bucket =
        getRuleBucket(regionRule.getId(), regionRule.getActionType());
    if (ruleType.isEmpty() || bucket.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        BlockingPolicyData.builder()
            .category(Category.CUSTOM_REGION_RULE)
            .bucket(bucket.get())
            .ruleType(ruleType.get())
            .info(
                ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo(
                    regionRule.getId(), regionRule.getName()))
            .timestamp(regionRule.getExpirationDetails().getTimestampMillis())
            .status(
                blockingRulesUtils.generateBlockingStatus(
                    regionRule.getExpirationDetails().getTimestampMillis(), ruleType.get()))
            .blockingDetails(
                RegionBlockingDetails.builder()
                    .regions(
                        regionRule.getRegionIdToCountryMapMap().values().stream()
                            .map(Country::getIsoCode)
                            .collect(Collectors.toUnmodifiableList()))
                    .build())
            .build());
  }

  private static Optional<BlockingPolicyDataBucket> getRuleBucket(
      String id, RegionRuleActionType actionType) {
    switch (actionType) {
      case REGION_RULE_ACTION_TYPE_BLOCK:
        return Optional.of(REGION_VIOLATIONS);
      case REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT:
        return Optional.of(BlockingPolicyDataBucket.REGION_BLOCK_ALL_EXCEPT_VIOLATIONS);
      default:
        log.info(
            "No bucket type exist for rule with rule type : {} and rule id {}", actionType, id);
        return Optional.empty();
    }
  }

  private static Optional<BlockingPolicyData.RuleType> getRuleType(
      String id, RegionRuleActionType actionType) {
    switch (actionType) {
      case REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT:
        return Optional.of(BlockingPolicyData.RuleType.BLOCK_ALL_EXCEPT);
      case REGION_RULE_ACTION_TYPE_BLOCK:
        return Optional.of(BlockingPolicyData.RuleType.BLOCK);
      default:
        log.error("Invalid rule action type: {} for rule with rule id: {}", actionType, id);
        return Optional.empty();
    }
  }
}
