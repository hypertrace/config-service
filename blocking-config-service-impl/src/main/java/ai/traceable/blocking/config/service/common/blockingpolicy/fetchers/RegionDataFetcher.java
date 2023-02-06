package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.RuleType;
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
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class RegionDataFetcher {
  private static final List<RegionRuleActionType> SUPPORTED_RULE_ACTIONS =
      ImmutableList.of(REGION_RULE_ACTION_TYPE_BLOCK, REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT);

  private final RegionConfigServiceBlockingStub regionConfigServiceStub;
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  public RegionDataFetcher(
      RegionConfigServiceBlockingStub regionConfigServiceStub,
      BlockingRulesUtils blockingRulesUtils) {
    this.regionConfigServiceStub = regionConfigServiceStub;
    this.blockingRulesUtils = blockingRulesUtils;
  }

  public Map<RuleType, List<BlockingPolicyData>> getRegionBasedRules(
      RequestContext requestContext, Optional<String> environmentId) {
    List<BlockingPolicyData> violations = new ArrayList<>();
    List<BlockingPolicyData> blockAllExcepts = new ArrayList<>();

    // empty env scope will only return rules with rule-scope as all-envs
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

    response.getRuleList().stream()
        .filter(this::filterRule)
        .forEach(
            regionRule -> {
              switch (regionRule.getActionType()) {
                case REGION_RULE_ACTION_TYPE_BLOCK:
                  violations.add(this.generateBlockingDetails(regionRule, RuleType.BLOCK));
                  break;
                case REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT:
                  blockAllExcepts.add(
                      this.generateBlockingDetails(regionRule, RuleType.BLOCK_ALL_EXCEPT));
                  break;
                default:
                  log.warn(
                      "Unsupported region based rule event type {} for request-context:{} and ruleID:{}",
                      regionRule.getActionType(),
                      requestContext,
                      regionRule.getId());
              }
            });
    return new EnumMap<>(
        Map.of(
            RuleType.BLOCK, violations,
            RuleType.BLOCK_ALL_EXCEPT, blockAllExcepts));
  }

  private boolean filterRule(RegionRule regionRule) {
    return !regionRule.getRegionIdList().isEmpty()
        && blockingRulesUtils.isRuleActive(regionRule.getExpirationDetails().getTimestampMillis());
  }

  private BlockingPolicyData generateBlockingDetails(RegionRule regionRule, RuleType ruleType) {
    return BlockingPolicyData.builder()
        .category(Category.CUSTOM_REGION_RULE)
        .ruleType(ruleType)
        .info(
            ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo(
                regionRule.getId(), regionRule.getName()))
        .timestamp(regionRule.getExpirationDetails().getTimestampMillis())
        .status(
            blockingRulesUtils.generateBlockingStatus(
                regionRule.getExpirationDetails().getTimestampMillis(), ruleType))
        .regions(regionRule.getRegionIdList())
        .build();
  }
}
