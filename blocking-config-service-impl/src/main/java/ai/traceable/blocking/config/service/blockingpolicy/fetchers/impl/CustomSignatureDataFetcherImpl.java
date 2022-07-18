package ai.traceable.blocking.config.service.blockingpolicy.fetchers.impl;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.CustomSignatureDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.CustomSignatureDetails;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.google.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

public class CustomSignatureDataFetcherImpl implements CustomSignatureDataFetcher {
  private final CustomSignatureConfigServiceBlockingStub configServiceBlockingStub;
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  public CustomSignatureDataFetcherImpl(
      CustomSignatureConfigServiceBlockingStub configServiceBlockingStub,
      BlockingRulesUtils blockingRulesUtils) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.blockingRulesUtils = blockingRulesUtils;
  }

  @Override
  public List<BlockingDetails> getCustomSignatureExemptions() {
    GetCustomSignatureRulesResponse response =
        configServiceBlockingStub.getCustomSignatureRules(
            GetCustomSignatureRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .addEventTypes(EventType.EVENT_TYPE_ALLOW)
                        .setDisabled(false)
                        .build())
                .build());

    return response.getRulesList().stream()
        .filter(
            customSignatureRule ->
                blockingRulesUtils.isRuleActive(
                    customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis()))
        .map(
            customSignatureRule ->
                BlockingDetails.newBuilder()
                    .setCategory(BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE)
                    .setBlockingRuleType(BLOCKING_RULE_TYPE_ALLOW)
                    .setInfo(
                        ExemptionInfoEncoder.getEncodedCustomSignatureRuleExemptionInfo(
                            customSignatureRule.getId(),
                            customSignatureRule.getName(),
                            customSignatureRule.getEffect().getEventSeverity().name()))
                    .setExpirationTimestamp(
                        customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis())
                    .setStatus(
                        blockingRulesUtils.generateBlockingStatus(
                            customSignatureRule
                                .getBlockingExpiryDetails()
                                .getExpiryTimestampMillis(),
                            BLOCKING_RULE_TYPE_ALLOW))
                    .setCustomSignatureDetails(
                        CustomSignatureDetails.newBuilder()
                            .setRuleId(customSignatureRule.getId())
                            .build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<BlockingDetails> getCustomSignatureViolations() {
    GetCustomSignatureRulesResponse response =
        configServiceBlockingStub.getCustomSignatureRules(
            GetCustomSignatureRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .addEventTypes(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                        .setDisabled(false)
                        .build())
                .build());

    return response.getRulesList().stream()
        .filter(
            customSignatureRule ->
                blockingRulesUtils.isRuleActive(
                    customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis()))
        .map(
            customSignatureRule ->
                BlockingDetails.newBuilder()
                    .setCategory(BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE)
                    .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK)
                    .setInfo(
                        ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
                            customSignatureRule.getId(),
                            customSignatureRule.getName(),
                            customSignatureRule.getEffect().getEventSeverity().name()))
                    .setExpirationTimestamp(
                        customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis())
                    .setStatus(
                        blockingRulesUtils.generateBlockingStatus(
                            customSignatureRule
                                .getBlockingExpiryDetails()
                                .getExpiryTimestampMillis(),
                            BLOCKING_RULE_TYPE_BLOCK))
                    .setCustomSignatureDetails(
                        CustomSignatureDetails.newBuilder()
                            .setRuleId(customSignatureRule.getId())
                            .build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }
}
