package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.google.protobuf.util.Timestamps;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MaliciousSourceDataHandler {
  private static final String NON_BLOCKING_RULE_INFO = "Rule has some non-blocking agent action";

  private final BlockingRulesUtils blockingRulesUtils;

  protected MaliciousSourceDataHandler(BlockingRulesUtils blockingRulesUtils) {
    this.blockingRulesUtils = blockingRulesUtils;
  }

  protected abstract BlockingPolicyData.Category getCategory();

  protected abstract Optional<BlockingPolicyDataBucket> getRuleBucket(
      String ruleId, RuleActionType ruleActionType);

  protected abstract BlockingDetails generateBlockingDetails(MaliciousSourcesRule rule);

  public Optional<BlockingPolicyData> getBlockingDetails(MaliciousSourcesRule rule) {
    long expirationTimestampMillis =
        Timestamps.toMillis(
            rule.getRuleInfo().getRuleAction().getExpirationDetails().getExpirationTimestamp());
    if (!blockingRulesUtils.isRuleActive(expirationTimestampMillis)) {
      return Optional.empty();
    }
    Optional<BlockingPolicyData.RuleType> ruleType =
        getRuleType(rule.getId(), rule.getRuleInfo().getRuleAction().getActionType());
    Optional<BlockingPolicyDataBucket> bucket =
        getRuleBucket(rule.getId(), rule.getRuleInfo().getRuleAction().getActionType());

    if (ruleType.isEmpty() || bucket.isEmpty()) {
      return Optional.empty();
    }
    BlockingPolicyData blockingPolicyData =
        BlockingPolicyData.builder()
            .ruleType(ruleType.get())
            .info(generateRuleInfo(rule))
            .timestamp(expirationTimestampMillis)
            .status(
                blockingRulesUtils.generateBlockingStatus(
                    expirationTimestampMillis, ruleType.get()))
            .category(getCategory())
            .bucket(bucket.get())
            .blockingDetails(generateBlockingDetails(rule))
            .action(
                MaliciousSourceRuleEffectConverter.convert(
                    rule.getRuleInfo().getRuleAction().getEffectsList()))
            .build();
    return Optional.of(blockingPolicyData);
  }

  private static String generateRuleInfo(MaliciousSourcesRule rule) {
    if (rule.getRuleInfo()
        .getRuleAction()
        .getActionType()
        .equals(RuleActionType.RULE_ACTION_TYPE_ALERT)) {
      return NON_BLOCKING_RULE_INFO;
    }
    return ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
        rule.getId(),
        rule.getRuleInfo().getName(),
        rule.getRuleInfo().getRuleAction().getEventSeverity().name(),
        Optional.empty(),
        List.of(rule.getRuleInfo().getConditions(0).getConditionCase()));
  }

  private static Optional<BlockingPolicyData.RuleType> getRuleType(
      String id, RuleActionType actionType) {
    switch (actionType) {
      case RULE_ACTION_TYPE_ALLOW:
        return Optional.of(BlockingPolicyData.RuleType.ALLOW);
      case RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT:
        return Optional.of(BlockingPolicyData.RuleType.BLOCK_ALL_EXCEPT);
      case RULE_ACTION_TYPE_BLOCK:
        return Optional.of(BlockingPolicyData.RuleType.BLOCK);
      case RULE_ACTION_TYPE_ALERT:
        return Optional.of(BlockingPolicyData.RuleType.ANALYTICS);
      default:
        log.error("Invalid rule action type: {} for rule with rule id: {}", actionType, id);
        return Optional.empty();
    }
  }
}
