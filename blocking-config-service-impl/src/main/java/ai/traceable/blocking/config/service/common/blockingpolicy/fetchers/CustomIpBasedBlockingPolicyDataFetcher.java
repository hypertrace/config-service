package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.IP_RANGE_BLOCK_ALL_EXCEPT_VIOLATIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.IP_RANGE_EXEMPTIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.IP_RANGE_VIOLATIONS;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_ALLOW;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_BLOCK;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.iprange.config.service.v1.EnvironmentScope;
import ai.traceable.iprange.config.service.v1.GetIpRangeRulesRequest;
import ai.traceable.iprange.config.service.v1.GetIpRangeRulesResponse;
import ai.traceable.iprange.config.service.v1.GetRulesFilter;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.RuleAction;
import ai.traceable.iprange.config.service.v1.RuleScope;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class CustomIpBasedBlockingPolicyDataFetcher implements BlockingPolicyDataFetcherBase {
  private static final List<RuleAction> SUPPORTED_RULE_ACTIONS =
      ImmutableList.of(RULE_ACTION_BLOCK, RULE_ACTION_ALLOW, RULE_ACTION_BLOCK_ALL_EXCEPT);
  private final IpRangeConfigServiceBlockingStub ipRangeConfigServiceStub;
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  CustomIpBasedBlockingPolicyDataFetcher(
      IpRangeConfigServiceBlockingStub ipRangeConfigServiceStub,
      BlockingRulesUtils blockingRulesUtils) {
    this.ipRangeConfigServiceStub = ipRangeConfigServiceStub;
    this.blockingRulesUtils = blockingRulesUtils;
  }

  @Override
  public List<BlockingPolicyData> getBlockingPolicyData(
      RequestContext requestContext, BlockingPolicyDataFilter filter) {
    Optional<String> environmentId = filter.getEnvironmentId();
    List<IpRangeRule> ruleList = fetchIpRangeRules(requestContext, environmentId);
    return ruleList.stream()
        .map(this::getBlockingDetails)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<BlockingPolicyData> getBlockingDetails(IpRangeRule ipRule) {
    if (!filterRule(ipRule)) {
      return Optional.empty();
    }

    Optional<BlockingPolicyData.RuleType> ruleType =
        getRuleType(ipRule.getId(), ipRule.getRuleDetails().getRuleAction());
    Optional<BlockingPolicyDataBucket> ruleBucket =
        getRuleBucket(ipRule.getId(), ipRule.getRuleDetails().getRuleAction());
    Optional<String> ruleInfo = getInfo(ipRule);
    if (ruleType.isEmpty() || ruleBucket.isEmpty() || ruleInfo.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        BlockingPolicyData.builder()
            .category(BlockingPolicyData.Category.CUSTOM_IP_RULE)
            .ruleType(ruleType.get())
            .info(ruleInfo.get())
            .bucket(ruleBucket.get())
            .status(
                blockingRulesUtils.generateBlockingStatus(
                    ipRule.getRuleDetails().getExpirationDetails().getExpirationTimestampMillis(),
                    ruleType.get()))
            .timestamp(
                ipRule.getRuleDetails().getExpirationDetails().getExpirationTimestampMillis())
            .blockingDetails(
                IpBlockingDetails.builder()
                    .ipAddresses(ipRule.getIpAddressesList())
                    .ipRanges(ipRule.getIpRangesList())
                    .build())
            .build());
  }

  private static Optional<String> getInfo(IpRangeRule ipRangeRule) {
    switch (ipRangeRule.getRuleDetails().getRuleAction()) {
      case RULE_ACTION_ALLOW:
        return Optional.of(
            ExemptionInfoEncoder.getEncodedCustomIpRuleExemptionInfo(
                ipRangeRule.getId(), ipRangeRule.getRuleDetails().getName()));
      case RULE_ACTION_BLOCK_ALL_EXCEPT:
      case RULE_ACTION_BLOCK:
        return Optional.of(
            ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo(
                ipRangeRule.getId(), ipRangeRule.getRuleDetails().getName()));
      default:
        log.info(
            "No info exist for rule with rule action type: {}  with rule id: {}",
            ipRangeRule.getRuleDetails().getRuleAction(),
            ipRangeRule.getId());
        return Optional.empty();
    }
  }

  private static Optional<BlockingPolicyDataBucket> getRuleBucket(
      String id, RuleAction ruleAction) {
    switch (ruleAction) {
      case RULE_ACTION_ALLOW:
        return Optional.of(IP_RANGE_EXEMPTIONS);
      case RULE_ACTION_BLOCK_ALL_EXCEPT:
        return Optional.of(IP_RANGE_BLOCK_ALL_EXCEPT_VIOLATIONS);
      case RULE_ACTION_BLOCK:
        return Optional.of(IP_RANGE_VIOLATIONS);
      default:
        log.info(
            "No Bucket exist for rule with rule action type: {}  with rule id: {}", ruleAction, id);
        return Optional.empty();
    }
  }

  private static Optional<BlockingPolicyData.RuleType> getRuleType(
      String id, RuleAction ruleAction) {
    switch (ruleAction) {
      case RULE_ACTION_ALLOW:
        return Optional.of(BlockingPolicyData.RuleType.ALLOW);
      case RULE_ACTION_BLOCK_ALL_EXCEPT:
        return Optional.of(BlockingPolicyData.RuleType.BLOCK_ALL_EXCEPT);
      case RULE_ACTION_BLOCK:
        return Optional.of(BlockingPolicyData.RuleType.BLOCK);
      default:
        log.info("Invalid rule action type: {} for rule with rule id: {}", ruleAction, id);
        return Optional.empty();
    }
  }

  private boolean filterRule(IpRangeRule ipRule) {
    return (!ipRule.getIpAddressesList().isEmpty() || !ipRule.getIpRangesList().isEmpty())
        && blockingRulesUtils.isRuleActive(
            ipRule.getRuleDetails().getExpirationDetails().getExpirationTimestampMillis());
  }

  private List<IpRangeRule> fetchIpRangeRules(
      RequestContext requestContext, Optional<String> environmentId) {
    // empty env scope will only return rules with rule-scope as all-environments
    GetIpRangeRulesRequest getIpRangeRulesRequest =
        GetIpRangeRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .setDisabled(false)
                    .addAllRuleActions(SUPPORTED_RULE_ACTIONS)
                    .setRuleScope(
                        RuleScope.newBuilder()
                            .setEnvironmentScope(
                                environmentId
                                    .map(id -> EnvironmentScope.newBuilder().addEnvironmentIds(id))
                                    .orElse(EnvironmentScope.newBuilder()))))
            .build();

    GetIpRangeRulesResponse response =
        requestContext.call(() -> ipRangeConfigServiceStub.getIpRangeRules(getIpRangeRulesRequest));
    return response.getRulesList();
  }
}
