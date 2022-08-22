package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_IP_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingRuleType;
import ai.traceable.blocking.config.service.v1.IpDetails;
import ai.traceable.iprange.config.service.v1.GetIpRangeRulesRequest;
import ai.traceable.iprange.config.service.v1.GetIpRangeRulesResponse;
import ai.traceable.iprange.config.service.v1.GetRulesFilter;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomIpBasedDataFetcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(CustomIpBasedDataFetcher.class);
  private static final GetIpRangeRulesRequest getIpRangeRulesRequest =
      GetIpRangeRulesRequest.newBuilder()
          .setFilter(GetRulesFilter.newBuilder().setDisabled(false).build())
          .build();

  private final IpRangeConfigServiceBlockingStub ipRangeConfigServiceStub;
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  public CustomIpBasedDataFetcher(
      IpRangeConfigServiceBlockingStub ipRangeConfigServiceStub,
      BlockingRulesUtils blockingRulesUtils) {
    this.ipRangeConfigServiceStub = ipRangeConfigServiceStub;
    this.blockingRulesUtils = blockingRulesUtils;
  }

  public Map<BlockingRuleType, List<BlockingDetails>> getCustomIpBasedRules(
      RequestContext requestContext) {
    List<BlockingDetails> exemptions = new ArrayList<>();
    List<BlockingDetails> violations = new ArrayList<>();
    List<BlockingDetails> blockAllExcepts = new ArrayList<>();

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
                          BLOCKING_RULE_TYPE_BLOCK,
                          ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo(
                              ipRangeRule.getId(), ipRangeRule.getRuleDetails().getName())));
                  break;
                case RULE_ACTION_ALLOW:
                  exemptions.add(
                      this.generateBlockingDetails(
                          ipRangeRule,
                          BLOCKING_RULE_TYPE_ALLOW,
                          ExemptionInfoEncoder.getEncodedCustomIpRuleExemptionInfo(
                              ipRangeRule.getId(), ipRangeRule.getRuleDetails().getName())));
                  break;
                case RULE_ACTION_BLOCK_ALL_EXCEPT:
                  blockAllExcepts.add(
                      generateBlockingDetails(
                          ipRangeRule,
                          BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT,
                          ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo(
                              ipRangeRule.getId(), ipRangeRule.getRuleDetails().getName())));
                  break;
                default:
                  LOGGER.warn(
                      "Unsupported custom ip based rule event type {}",
                      ipRangeRule.getRuleDetails().getRuleAction());
              }
            });

    Map<BlockingRuleType, List<BlockingDetails>> customIpBasedRulesMap = new HashMap<>();
    customIpBasedRulesMap.put(BLOCKING_RULE_TYPE_ALLOW, exemptions);
    customIpBasedRulesMap.put(BLOCKING_RULE_TYPE_BLOCK, violations);
    customIpBasedRulesMap.put(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT, blockAllExcepts);
    return customIpBasedRulesMap;
  }

  private boolean filterRule(IpRangeRule ipRule) {
    return (!ipRule.getIpAddressesList().isEmpty() || !ipRule.getIpRangesList().isEmpty())
        && blockingRulesUtils.isRuleActive(
            ipRule.getRuleDetails().getExpirationDetails().getExpirationTimestampMillis());
  }

  private BlockingDetails generateBlockingDetails(
      IpRangeRule ipRule, BlockingRuleType blockingRuleType, String info) {
    return BlockingDetails.newBuilder()
        .setCategory(BLOCKING_CATEGORY_CUSTOM_IP_RULE)
        .setBlockingRuleType(blockingRuleType)
        .setInfo(info)
        .setStatus(
            blockingRulesUtils.generateBlockingStatus(
                ipRule.getRuleDetails().getExpirationDetails().getExpirationTimestampMillis(),
                blockingRuleType))
        .setExpirationTimestamp(
            ipRule.getRuleDetails().getExpirationDetails().getExpirationTimestampMillis())
        .setIpDetails(
            IpDetails.newBuilder()
                .addAllIpAddresses(ipRule.getIpAddressesList())
                .addAllIpRanges(ipRule.getIpRangesList())
                .build())
        .build();
  }
}
