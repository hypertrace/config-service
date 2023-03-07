package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IpTypeDataHandler extends MaliciousSourceDataHandler {
  @Inject
  public IpTypeDataHandler(BlockingRulesUtils blockingRulesUtils) {
    super(blockingRulesUtils);
  }

  @Override
  protected BlockingPolicyData.Category getCategory() {
    return BlockingPolicyData.Category.IP_TYPE_RULE;
  }

  @Override
  protected List<String> getBlockingIpAddresses(MaliciousSourcesRule rule) {
    return Collections.emptyList();
  }

  @Override
  protected List<String> getBlockingCidrIpRanges(MaliciousSourcesRule rule) {
    return Collections.emptyList();
  }

  @Override
  protected List<IpLocationType> getBlockingIpTypes(MaliciousSourcesRule maliciousSourcesRule) {
    return maliciousSourcesRule.getRuleInfo().getConditionsList().stream()
        .flatMap(
            condition -> condition.getIpLocationTypeCondition().getIpLocationTypesList().stream())
        .distinct()
        .filter(Objects::nonNull)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  protected List<String> getBlockingRegions(MaliciousSourcesRule rule) {
    return Collections.emptyList();
  }

  @Override
  protected Optional<BlockingPolicyDataBucket> getRuleBucket(String id, RuleActionType actionType) {
    switch (actionType) {
      case RULE_ACTION_TYPE_BLOCK:
        return Optional.of(BlockingPolicyDataBucket.IP_TYPE_VIOLATIONS);
      default:
        log.error("Invalid rule action type: {} for rule with rule id: {}", actionType, id);
        return Optional.empty();
    }
  }
}
