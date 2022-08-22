package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_REGION_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingRuleType;
import ai.traceable.blocking.config.service.v1.RegionDetails;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetAllRegionRulesResponse;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionDataFetcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(RegionDataFetcher.class);
  private static final GetAllRegionRulesRequest getAllRegionRulesRequest =
      GetAllRegionRulesRequest.getDefaultInstance();

  private final RegionConfigServiceBlockingStub regionConfigServiceStub;
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  public RegionDataFetcher(
      RegionConfigServiceBlockingStub regionConfigServiceStub,
      BlockingRulesUtils blockingRulesUtils) {
    this.regionConfigServiceStub = regionConfigServiceStub;
    this.blockingRulesUtils = blockingRulesUtils;
  }

  public Map<BlockingRuleType, List<BlockingDetails>> getRegionBasedRules(
      RequestContext requestContext) {
    List<BlockingDetails> violations = new ArrayList<>();
    List<BlockingDetails> blockAllExcepts = new ArrayList<>();

    GetAllRegionRulesResponse response =
        requestContext.call(
            () -> regionConfigServiceStub.getAllRegionRules(getAllRegionRulesRequest));

    response.getRuleList().stream()
        .filter(this::filterRule)
        .forEach(
            regionRule -> {
              switch (regionRule.getActionType()) {
                case REGION_RULE_ACTION_TYPE_BLOCK:
                  violations.add(
                      this.generateBlockingDetails(regionRule, BLOCKING_RULE_TYPE_BLOCK));
                  break;
                case REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT:
                  blockAllExcepts.add(
                      this.generateBlockingDetails(
                          regionRule, BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT));
                  break;
                default:
                  LOGGER.warn(
                      "Unsupported region based rule event type {}", regionRule.getActionType());
              }
            });

    Map<BlockingRuleType, List<BlockingDetails>> regionBasedRulesMap = new HashMap<>();
    regionBasedRulesMap.put(BLOCKING_RULE_TYPE_BLOCK, violations);
    regionBasedRulesMap.put(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT, blockAllExcepts);
    return regionBasedRulesMap;
  }

  private boolean filterRule(RegionRule regionRule) {
    return !regionRule.getRegionIdList().isEmpty()
        && blockingRulesUtils.isRuleActive(regionRule.getExpirationDetails().getTimestampMillis());
  }

  private BlockingDetails generateBlockingDetails(
      RegionRule regionRule, BlockingRuleType blockingRuleType) {
    return BlockingDetails.newBuilder()
        .setCategory(BLOCKING_CATEGORY_CUSTOM_REGION_RULE)
        .setBlockingRuleType(blockingRuleType)
        .setInfo(
            ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo(
                regionRule.getId(), regionRule.getName()))
        .setExpirationTimestamp(regionRule.getExpirationDetails().getTimestampMillis())
        .setStatus(
            blockingRulesUtils.generateBlockingStatus(
                regionRule.getExpirationDetails().getTimestampMillis(), blockingRuleType))
        .setRegionDetails(
            RegionDetails.newBuilder().addAllRegions(regionRule.getRegionIdList()).build())
        .build();
  }
}
