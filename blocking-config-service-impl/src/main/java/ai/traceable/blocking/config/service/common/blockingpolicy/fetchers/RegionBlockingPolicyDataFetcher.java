package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.REGION_VIOLATIONS;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.region.config.service.v1.EnvironmentScope;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetAllRegionRulesResponse;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import ai.traceable.region.config.service.v1.RuleScope;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class RegionBlockingPolicyDataFetcher implements BlockingPolicyDataFetcherBase {
  private static final List<RegionRuleActionType> SUPPORTED_RULE_ACTIONS =
      ImmutableList.of(REGION_RULE_ACTION_TYPE_BLOCK, REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT);
  private final RegionConfigServiceBlockingStub regionConfigServiceStub;
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  RegionBlockingPolicyDataFetcher(
      RegionConfigServiceBlockingStub regionConfigServiceStub,
      BlockingRulesUtils blockingRulesUtils) {
    this.regionConfigServiceStub = regionConfigServiceStub;
    this.blockingRulesUtils = blockingRulesUtils;
  }

  @Override
  public List<BlockingPolicyData> getBlockingPolicyData(
      RequestContext requestContext, BlockingPolicyDataFilter filter) {
    Optional<String> environmentId = filter.getEnvironmentId();
    List<RegionRule> ruleList = fetchRegionRules(requestContext, environmentId);
    return ruleList.stream()
        .map(this::getBlockingDetails)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toUnmodifiableList());
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
                RegionBlockingDetails.builder().regions(regionRule.getRegionIdList()).build())
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

  private List<RegionRule> fetchRegionRules(
      RequestContext requestContext, Optional<String> environmentId) {
    GetAllRegionRulesRequest rulesRequest =
        GetAllRegionRulesRequest.newBuilder()
            .setFilter(
                GetRegionRulesFilter.newBuilder()
                    .setDisabled(false)
                    .addAllRuleActionTypes(SUPPORTED_RULE_ACTIONS)
                    .setRuleScope(
                        RuleScope.newBuilder()
                            .setEnvironmentScope(
                                environmentId
                                    .map(id -> EnvironmentScope.newBuilder().addEnvironmentIds(id))
                                    .orElse(EnvironmentScope.newBuilder()))))
            .build();

    GetAllRegionRulesResponse response =
        requestContext.call(() -> regionConfigServiceStub.getAllRegionRules(rulesRequest));
    return response.getRuleList();
  }
}
