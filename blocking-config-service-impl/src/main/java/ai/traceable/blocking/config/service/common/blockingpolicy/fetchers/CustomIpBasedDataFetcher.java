package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_ALLOW;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_BLOCK;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.RuleType;
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
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomIpBasedDataFetcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(CustomIpBasedDataFetcher.class);
  private static final List<RuleAction> SUPPORTED_RULE_ACTIONS =
      ImmutableList.of(RULE_ACTION_BLOCK, RULE_ACTION_ALLOW, RULE_ACTION_BLOCK_ALL_EXCEPT);

  private final IpRangeConfigServiceBlockingStub ipRangeConfigServiceStub;
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  public CustomIpBasedDataFetcher(
      IpRangeConfigServiceBlockingStub ipRangeConfigServiceStub,
      BlockingRulesUtils blockingRulesUtils) {
    this.ipRangeConfigServiceStub = ipRangeConfigServiceStub;
    this.blockingRulesUtils = blockingRulesUtils;
  }

  public Map<RuleType, List<BlockingPolicyData>> getCustomIpBasedRules(
      RequestContext requestContext, Optional<String> environmentId) {
    List<BlockingPolicyData> exemptions = new ArrayList<>();
    List<BlockingPolicyData> violations = new ArrayList<>();
    List<BlockingPolicyData> blockAllExcepts = new ArrayList<>();

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

    response.getRulesList().stream()
        .filter(this::filterRule)
        .forEach(
            ipRangeRule -> {
              switch (ipRangeRule.getRuleDetails().getRuleAction()) {
                case RULE_ACTION_BLOCK:
                  violations.add(
                      this.generateBlockingDetails(
                          ipRangeRule,
                          RuleType.BLOCK,
                          ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo(
                              ipRangeRule.getId(), ipRangeRule.getRuleDetails().getName())));
                  break;
                case RULE_ACTION_ALLOW:
                  exemptions.add(
                      this.generateBlockingDetails(
                          ipRangeRule,
                          RuleType.ALLOW,
                          ExemptionInfoEncoder.getEncodedCustomIpRuleExemptionInfo(
                              ipRangeRule.getId(), ipRangeRule.getRuleDetails().getName())));
                  break;
                case RULE_ACTION_BLOCK_ALL_EXCEPT:
                  blockAllExcepts.add(
                      generateBlockingDetails(
                          ipRangeRule,
                          RuleType.BLOCK_ALL_EXCEPT,
                          ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo(
                              ipRangeRule.getId(), ipRangeRule.getRuleDetails().getName())));
                  break;
                default:
                  LOGGER.warn(
                      "Unsupported custom ip based rule event type {} for request-context:{} and ruleID:{}",
                      ipRangeRule.getRuleDetails().getRuleAction(),
                      requestContext,
                      ipRangeRule.getId());
              }
            });

    return new EnumMap<>(
        Map.of(
            RuleType.ALLOW, exemptions,
            RuleType.BLOCK, violations,
            RuleType.BLOCK_ALL_EXCEPT, blockAllExcepts));
  }

  private boolean filterRule(IpRangeRule ipRule) {
    return (!ipRule.getIpAddressesList().isEmpty() || !ipRule.getIpRangesList().isEmpty())
        && blockingRulesUtils.isRuleActive(
            ipRule.getRuleDetails().getExpirationDetails().getExpirationTimestampMillis());
  }

  private BlockingPolicyData generateBlockingDetails(
      IpRangeRule ipRule, RuleType ruleType, String info) {
    return BlockingPolicyData.builder()
        .category(Category.CUSTOM_IP_RULE)
        .ruleType(ruleType)
        .info(info)
        .status(
            blockingRulesUtils.generateBlockingStatus(
                ipRule.getRuleDetails().getExpirationDetails().getExpirationTimestampMillis(),
                ruleType))
        .timestamp(ipRule.getRuleDetails().getExpirationDetails().getExpirationTimestampMillis())
        .ipAddresses(ipRule.getIpAddressesList())
        .ipRanges(ipRule.getIpRangesList())
        .build();
  }
}
