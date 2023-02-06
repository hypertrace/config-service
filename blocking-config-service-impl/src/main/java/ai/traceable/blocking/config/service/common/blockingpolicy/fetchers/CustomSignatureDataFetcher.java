package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.RuleType;
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
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.EnumMap;
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

  public Map<RuleType, List<BlockingPolicyData>> getCustomSignatureRules(
      RequestContext requestContext, Optional<String> environmentId) {
    List<BlockingPolicyData> exemptions = new ArrayList<>();
    List<BlockingPolicyData> violations = new ArrayList<>();

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
        .filter(this::filterRule)
        .forEach(
            customSignatureRule -> {
              switch (customSignatureRule.getEffect().getEventType()) {
                case EVENT_TYPE_ALLOW:
                  exemptions.add(
                      this.generateBlockingDetails(
                          customSignatureRule,
                          RuleType.ALLOW,
                          ExemptionInfoEncoder.getEncodedCustomSignatureRuleExemptionInfo(
                              customSignatureRule.getId(),
                              customSignatureRule.getName(),
                              customSignatureRule.getEffect().getEventSeverity().name())));
                  break;
                case EVENT_TYPE_DETECTION_AND_BLOCKING:
                  violations.add(
                      this.generateBlockingDetails(
                          customSignatureRule,
                          RuleType.BLOCK,
                          ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
                              customSignatureRule.getId(),
                              customSignatureRule.getName(),
                              customSignatureRule.getEffect().getEventSeverity().name())));
                  break;
                default:
                  LOGGER.warn(
                      "Unsupported custom signature based rule event type {} for request-context:{} and ruleID:{}",
                      customSignatureRule.getEffect().getEventType(),
                      requestContext,
                      customSignatureRule.getId());
              }
            });

    return new EnumMap<>(Map.of(RuleType.ALLOW, exemptions, RuleType.BLOCK, violations));
  }

  private boolean filterRule(CustomSignatureRule customSignatureRule) {
    return blockingRulesUtils.isRuleActive(
        customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis());
  }

  private BlockingPolicyData generateBlockingDetails(
      CustomSignatureRule customSignatureRule, RuleType ruleType, String info) {
    return BlockingPolicyData.builder()
        .category(Category.CUSTOM_SIGNATURE_RULE)
        .ruleType(ruleType)
        .info(info)
        .timestamp(customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis())
        .status(
            blockingRulesUtils.generateBlockingStatus(
                customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis(),
                ruleType))
        .ruleId(customSignatureRule.getId())
        .build();
  }
}
