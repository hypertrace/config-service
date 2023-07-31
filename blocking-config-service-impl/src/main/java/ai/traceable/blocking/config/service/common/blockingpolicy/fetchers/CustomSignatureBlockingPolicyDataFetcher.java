package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.CUSTOM_SIGNATURE_EXEMPTIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.CUSTOM_SIGNATURE_VIOLATIONS;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CustomSignatureBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleScope;
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
class CustomSignatureBlockingPolicyDataFetcher implements BlockingPolicyDataFetcherBase {
  private final CustomSignatureConfigServiceBlockingStub configServiceBlockingStub;
  private static final List<EventType> EVENT_TYPES_LIST =
      ImmutableList.of(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING, EventType.EVENT_TYPE_ALLOW);
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  CustomSignatureBlockingPolicyDataFetcher(
      CustomSignatureConfigServiceBlockingStub configServiceBlockingStub,
      BlockingRulesUtils blockingRulesUtils) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.blockingRulesUtils = blockingRulesUtils;
  }

  @Override
  public List<BlockingPolicyData> getBlockingPolicyData(
      RequestContext requestContext, BlockingPolicyDataFilter filter) {
    Optional<String> environmentId = filter.getEnvironmentId();
    List<CustomSignatureRule> ruleList = fetchCustomSignatureRule(requestContext, environmentId);
    return ruleList.stream()
        .map(this::getBlockingDetails)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<BlockingPolicyData> getBlockingDetails(CustomSignatureRule customSignatureRule) {
    if (!blockingRulesUtils.isRuleActive(
        customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis())) {
      return Optional.empty();
    }
    Optional<BlockingPolicyData.RuleType> ruleType =
        getRuleType(customSignatureRule.getId(), customSignatureRule.getEffect().getEventType());
    Optional<BlockingPolicyDataBucket> ruleBucket =
        getRuleBucket(customSignatureRule.getEffect().getEventType());
    Optional<String> info = getInfo(customSignatureRule);
    if (ruleType.isEmpty() || ruleBucket.isEmpty() || info.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        BlockingPolicyData.builder()
            .category(Category.CUSTOM_SIGNATURE_RULE)
            .ruleType(ruleType.get())
            .bucket(ruleBucket.get())
            .info(info.get())
            .timestamp(customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis())
            .status(
                blockingRulesUtils.generateBlockingStatus(
                    customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis(),
                    ruleType.get()))
            .blockingDetails(
                CustomSignatureBlockingDetails.builder()
                    .ruleId(customSignatureRule.getId())
                    .build())
            .build());
  }

  private static Optional<String> getInfo(CustomSignatureRule rule) {
    switch (rule.getEffect().getEventType()) {
      case EVENT_TYPE_ALLOW:
        return Optional.of(
            ExemptionInfoEncoder.getEncodedCustomSignatureRuleExemptionInfo(
                rule.getId(), rule.getName(), rule.getEffect().getEventSeverity().name()));

      case EVENT_TYPE_DETECTION_AND_BLOCKING:
        return Optional.of(
            ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
                rule.getId(), rule.getName(), rule.getEffect().getEventSeverity().name()));

      default:
        log.info("Could not find info for rule with rule id: {}", rule.getId());
        return Optional.empty();
    }
  }

  private static Optional<BlockingPolicyDataBucket> getRuleBucket(EventType eventType) {
    switch (eventType) {
      case EVENT_TYPE_ALLOW:
        return Optional.of(CUSTOM_SIGNATURE_EXEMPTIONS);
      case EVENT_TYPE_DETECTION_AND_BLOCKING:
        return Optional.of(CUSTOM_SIGNATURE_VIOLATIONS);
      default:
        log.info("No bucket type exist for event type : {}", eventType);
        return Optional.empty();
    }
  }

  private static Optional<BlockingPolicyData.RuleType> getRuleType(String id, EventType eventType) {
    switch (eventType) {
      case EVENT_TYPE_ALLOW:
        return Optional.of(BlockingPolicyData.RuleType.ALLOW);
      case EVENT_TYPE_DETECTION_AND_BLOCKING:
        return Optional.of(BlockingPolicyData.RuleType.BLOCK);
      default:
        log.info("Invalid rule event type: {} for rule with rule id: {}", eventType, id);
        return Optional.empty();
    }
  }

  private List<CustomSignatureRule> fetchCustomSignatureRule(
      RequestContext requestContext, Optional<String> environmentId) {
    // empty env scope will only return rules with rule-scope as all-envs
    GetCustomSignatureRulesRequest getCustomSignatureRulesRequest =
        GetCustomSignatureRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .addAllEventTypes(EVENT_TYPES_LIST)
                    .setDisabled(false)
                    .setRuleScope(
                        RuleScope.newBuilder()
                            .setEnvironmentScope(
                                environmentId
                                    .map(id -> EnvironmentScope.newBuilder().addEnvironmentIds(id))
                                    .orElse(EnvironmentScope.newBuilder()))))
            .build();

    GetCustomSignatureRulesResponse response =
        requestContext.call(
            () ->
                configServiceBlockingStub.getCustomSignatureRules(getCustomSignatureRulesRequest));
    return response.getRulesList();
  }
}
