package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingRuleType;
import ai.traceable.blocking.config.service.v1.CustomSignatureDetails;
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
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomSignatureDataFetcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(CustomSignatureDataFetcher.class);
  private static final List<EventType> EVENT_TYPES_LIST =
      ImmutableList.of(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING, EventType.EVENT_TYPE_ALLOW);

  private final CustomSignatureConfigServiceBlockingStub configServiceBlockingStub;
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  public CustomSignatureDataFetcher(
      CustomSignatureConfigServiceBlockingStub configServiceBlockingStub,
      BlockingRulesUtils blockingRulesUtils) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.blockingRulesUtils = blockingRulesUtils;
  }

  public Map<BlockingRuleType, List<BlockingDetails>> getCustomSignatureRules(
      RequestContext requestContext, Optional<String> environmentId) {
    List<BlockingDetails> exemptions = new ArrayList<>();
    List<BlockingDetails> violations = new ArrayList<>();

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

    response.getRulesList().stream()
        .filter(customSignatureRule -> this.filterRule(customSignatureRule, environmentId))
        .forEach(
            customSignatureRule -> {
              switch (customSignatureRule.getEffect().getEventType()) {
                case EVENT_TYPE_ALLOW:
                  exemptions.add(
                      this.generateBlockingDetails(
                          customSignatureRule,
                          BLOCKING_RULE_TYPE_ALLOW,
                          ExemptionInfoEncoder.getEncodedCustomSignatureRuleExemptionInfo(
                              customSignatureRule.getId(),
                              customSignatureRule.getName(),
                              customSignatureRule.getEffect().getEventSeverity().name())));
                  break;
                case EVENT_TYPE_DETECTION_AND_BLOCKING:
                  violations.add(
                      this.generateBlockingDetails(
                          customSignatureRule,
                          BLOCKING_RULE_TYPE_BLOCK,
                          ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
                              customSignatureRule.getId(),
                              customSignatureRule.getName(),
                              customSignatureRule.getEffect().getEventSeverity().name())));
                  break;
                default:
                  LOGGER.warn(
                      "Unsupported custom signature rule event type {}",
                      customSignatureRule.getEffect().getEventType());
              }
            });

    Map<BlockingRuleType, List<BlockingDetails>> customSignatureRulesMap = new HashMap<>();
    customSignatureRulesMap.put(BLOCKING_RULE_TYPE_ALLOW, exemptions);
    customSignatureRulesMap.put(BLOCKING_RULE_TYPE_BLOCK, violations);
    return customSignatureRulesMap;
  }

  private boolean filterRule(
      CustomSignatureRule customSignatureRule, Optional<String> environmentId) {
    return blockingRulesUtils.isRuleActive(
        customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis());
  }

  private BlockingDetails generateBlockingDetails(
      CustomSignatureRule customSignatureRule, BlockingRuleType blockingRuleType, String info) {
    return BlockingDetails.newBuilder()
        .setCategory(BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE)
        .setBlockingRuleType(blockingRuleType)
        .setInfo(info)
        .setExpirationTimestamp(
            customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis())
        .setStatus(
            blockingRulesUtils.generateBlockingStatus(
                customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis(),
                blockingRuleType))
        .setCustomSignatureDetails(
            CustomSignatureDetails.newBuilder().setRuleId(customSignatureRule.getId()))
        .build();
  }
}
