package ai.traceable.blocking.config.service.blockingpolicy.fetchers.impl;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_REGION_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.RegionDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.RegionDetails;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetAllRegionRulesResponse;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import com.google.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

public class RegionDataFetcherImpl implements RegionDataFetcher {
  private final RegionConfigServiceBlockingStub regionConfigServiceStub;
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  public RegionDataFetcherImpl(
      RegionConfigServiceBlockingStub regionConfigServiceStub,
      BlockingRulesUtils blockingRulesUtils) {
    this.regionConfigServiceStub = regionConfigServiceStub;
    this.blockingRulesUtils = blockingRulesUtils;
  }

  @Override
  public List<BlockingDetails> getRegionViolations() {
    GetAllRegionRulesResponse response =
        regionConfigServiceStub.getAllRegionRules(GetAllRegionRulesRequest.getDefaultInstance());

    return response.getRuleList().stream()
        .filter(regionRule -> regionRule.getActionType() == REGION_RULE_ACTION_TYPE_BLOCK)
        .filter(regionRule -> !regionRule.getRegionIdList().isEmpty())
        .filter(
            regionRule ->
                blockingRulesUtils.isRuleActive(
                    regionRule.getExpirationDetails().getTimestampMillis()))
        .map(
            regionRule ->
                BlockingDetails.newBuilder()
                    .setCategory(BLOCKING_CATEGORY_CUSTOM_REGION_RULE)
                    .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK)
                    .setInfo(
                        ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo(
                            regionRule.getId(), regionRule.getName()))
                    .setExpirationTimestamp(regionRule.getExpirationDetails().getTimestampMillis())
                    .setStatus(
                        blockingRulesUtils.generateBlockingStatus(
                            regionRule.getExpirationDetails().getTimestampMillis(),
                            BLOCKING_RULE_TYPE_BLOCK))
                    .setRegionDetails(
                        RegionDetails.newBuilder()
                            .addAllRegions(regionRule.getRegionIdList())
                            .build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<BlockingDetails> getRegionViolationAllExcepts() {
    GetAllRegionRulesResponse response =
        regionConfigServiceStub.getAllRegionRules(GetAllRegionRulesRequest.getDefaultInstance());

    return response.getRuleList().stream()
        .filter(
            regionRule -> regionRule.getActionType() == REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
        .filter(regionRule -> !regionRule.getRegionIdList().isEmpty())
        .filter(
            regionRule ->
                blockingRulesUtils.isRuleActive(
                    regionRule.getExpirationDetails().getTimestampMillis()))
        .map(
            regionRule ->
                BlockingDetails.newBuilder()
                    .setCategory(BLOCKING_CATEGORY_CUSTOM_REGION_RULE)
                    .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT)
                    .setInfo(
                        ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo(
                            regionRule.getId(), regionRule.getName()))
                    .setExpirationTimestamp(regionRule.getExpirationDetails().getTimestampMillis())
                    .setStatus(
                        blockingRulesUtils.generateBlockingStatus(
                            regionRule.getExpirationDetails().getTimestampMillis(),
                            BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT))
                    .setRegionDetails(
                        RegionDetails.newBuilder()
                            .addAllRegions(regionRule.getRegionIdList())
                            .build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }
}
