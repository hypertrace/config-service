package ai.traceable.blocking.config.service.blockingpolicy.fetchers.impl;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_IP_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_ALLOW;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_BLOCK;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.CustomIpBasedDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.IpDetails;
import ai.traceable.iprange.config.service.v1.GetIpRangeRulesRequest;
import ai.traceable.iprange.config.service.v1.GetIpRangeRulesResponse;
import ai.traceable.iprange.config.service.v1.GetRulesFilter;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.google.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

public class CustomIpBasedDataFetcherImpl implements CustomIpBasedDataFetcher {
  private final IpRangeConfigServiceBlockingStub ipRangeConfigServiceStub;
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  public CustomIpBasedDataFetcherImpl(
      IpRangeConfigServiceBlockingStub ipRangeConfigServiceStub,
      BlockingRulesUtils blockingRulesUtils) {
    this.ipRangeConfigServiceStub = ipRangeConfigServiceStub;
    this.blockingRulesUtils = blockingRulesUtils;
  }

  @Override
  public List<BlockingDetails> getCustomIpBasedViolations() {
    GetIpRangeRulesResponse response =
        ipRangeConfigServiceStub.getIpRangeRules(
            GetIpRangeRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .setRuleAction(RULE_ACTION_BLOCK)
                        .setDisabled(false)
                        .build())
                .build());

    return response.getRulesList().stream()
        .filter(
            ipRule -> !ipRule.getIpAddressesList().isEmpty() || !ipRule.getIpRangesList().isEmpty())
        .filter(
            ipRule ->
                blockingRulesUtils.isRuleActive(
                    ipRule.getRuleDetails().getExpirationDetails().getExpirationTimestampMillis()))
        .map(
            ipRule ->
                BlockingDetails.newBuilder()
                    .setCategory(BLOCKING_CATEGORY_CUSTOM_IP_RULE)
                    .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK)
                    .setInfo(
                        ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo(
                            ipRule.getId(), ipRule.getRuleDetails().getName()))
                    .setExpirationTimestamp(
                        ipRule
                            .getRuleDetails()
                            .getExpirationDetails()
                            .getExpirationTimestampMillis())
                    .setStatus(
                        blockingRulesUtils.generateBlockingStatus(
                            ipRule
                                .getRuleDetails()
                                .getExpirationDetails()
                                .getExpirationTimestampMillis(),
                            BLOCKING_RULE_TYPE_BLOCK))
                    .setIpDetails(
                        IpDetails.newBuilder()
                            .addAllIpAddresses(ipRule.getIpAddressesList())
                            .addAllIpRanges(ipRule.getIpRangesList())
                            .build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<BlockingDetails> getCustomIpBasedExemptions() {
    GetIpRangeRulesResponse response =
        ipRangeConfigServiceStub.getIpRangeRules(
            GetIpRangeRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .setRuleAction(RULE_ACTION_ALLOW)
                        .setDisabled(false)
                        .build())
                .build());

    return response.getRulesList().stream()
        .filter(
            ipRule -> !ipRule.getIpAddressesList().isEmpty() || !ipRule.getIpRangesList().isEmpty())
        .filter(
            ipRule ->
                blockingRulesUtils.isRuleActive(
                    ipRule.getRuleDetails().getExpirationDetails().getExpirationTimestampMillis()))
        .map(
            ipRule ->
                BlockingDetails.newBuilder()
                    .setCategory(BLOCKING_CATEGORY_CUSTOM_IP_RULE)
                    .setBlockingRuleType(BLOCKING_RULE_TYPE_ALLOW)
                    .setInfo(
                        ExemptionInfoEncoder.getEncodedCustomIpRuleExemptionInfo(
                            ipRule.getId(), ipRule.getRuleDetails().getName()))
                    .setExpirationTimestamp(
                        ipRule
                            .getRuleDetails()
                            .getExpirationDetails()
                            .getExpirationTimestampMillis())
                    .setStatus(
                        blockingRulesUtils.generateBlockingStatus(
                            ipRule
                                .getRuleDetails()
                                .getExpirationDetails()
                                .getExpirationTimestampMillis(),
                            BLOCKING_RULE_TYPE_ALLOW))
                    .setIpDetails(
                        IpDetails.newBuilder()
                            .addAllIpAddresses(ipRule.getIpAddressesList())
                            .addAllIpRanges(ipRule.getIpRangesList())
                            .build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<BlockingDetails> getCustomIpBasedBlockAllExcepts() {
    GetIpRangeRulesResponse response =
        ipRangeConfigServiceStub.getIpRangeRules(
            GetIpRangeRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .setRuleAction(RULE_ACTION_BLOCK_ALL_EXCEPT)
                        .setDisabled(false)
                        .build())
                .build());

    return response.getRulesList().stream()
        .filter(
            ipRule -> !ipRule.getIpAddressesList().isEmpty() || !ipRule.getIpRangesList().isEmpty())
        .filter(
            ipRule ->
                blockingRulesUtils.isRuleActive(
                    ipRule.getRuleDetails().getExpirationDetails().getExpirationTimestampMillis()))
        .map(
            ipRule ->
                BlockingDetails.newBuilder()
                    .setCategory(BLOCKING_CATEGORY_CUSTOM_IP_RULE)
                    .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT)
                    .setInfo(
                        ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo(
                            ipRule.getId(), ipRule.getRuleDetails().getName()))
                    .setStatus(
                        blockingRulesUtils.generateBlockingStatus(
                            ipRule
                                .getRuleDetails()
                                .getExpirationDetails()
                                .getExpirationTimestampMillis(),
                            BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT))
                    .setExpirationTimestamp(
                        ipRule
                            .getRuleDetails()
                            .getExpirationDetails()
                            .getExpirationTimestampMillis())
                    .setIpDetails(
                        IpDetails.newBuilder()
                            .addAllIpAddresses(ipRule.getIpAddressesList())
                            .addAllIpRanges(ipRule.getIpRangesList())
                            .build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }
}
