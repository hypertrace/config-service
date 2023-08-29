package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IpRangeDataHandler extends MaliciousSourceDataHandler {

  @Inject
  public IpRangeDataHandler(BlockingRulesUtils blockingRulesUtils) {
    super(blockingRulesUtils);
  }

  @Override
  protected Category getCategory() {
    return Category.CUSTOM_IP_RULE;
  }

  @Override
  protected Optional<BlockingPolicyDataBucket> getRuleBucket(String id, RuleActionType actionType) {
    switch (actionType) {
      case RULE_ACTION_TYPE_ALLOW:
        return Optional.of(BlockingPolicyDataBucket.IP_RANGE_EXEMPTIONS);
      case RULE_ACTION_TYPE_BLOCK:
        return Optional.of(BlockingPolicyDataBucket.IP_RANGE_VIOLATIONS);
      case RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT:
        return Optional.of(BlockingPolicyDataBucket.IP_RANGE_BLOCK_ALL_EXCEPT_VIOLATIONS);
      case RULE_ACTION_TYPE_ALERT:
        return Optional.empty();
      default:
        log.error("Invalid rule action type: {} for rule with rule id: {}", actionType, id);
        return Optional.empty();
    }
  }

  @Override
  protected BlockingDetails generateBlockingDetails(MaliciousSourcesRule rule) {
    return IpBlockingDetails.builder()
        .ipAddresses(
            rule.getRuleInfo().getConditionsList().stream()
                .flatMap(condition -> condition.getIpRangeCondition().getIpAddressesList().stream())
                .distinct()
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableList()))
        .ipRanges(
            rule.getRuleInfo().getConditionsList().stream()
                .flatMap(
                    condition -> condition.getIpRangeCondition().getCidrIpRangesList().stream())
                .distinct()
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }
}
