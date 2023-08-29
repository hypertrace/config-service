package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpTypeBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
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
  protected BlockingDetails generateBlockingDetails(MaliciousSourcesRule maliciousSourcesRule) {
    return IpTypeBlockingDetails.builder()
        .ipTypes(
            maliciousSourcesRule.getRuleInfo().getConditionsList().stream()
                .flatMap(
                    condition ->
                        condition.getIpLocationTypeCondition().getIpLocationTypesList().stream())
                .distinct()
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }

  @Override
  protected Optional<BlockingPolicyDataBucket> getRuleBucket(String id, RuleActionType actionType) {
    switch (actionType) {
      case RULE_ACTION_TYPE_BLOCK:
        return Optional.of(BlockingPolicyDataBucket.IP_TYPE_VIOLATIONS);
      case RULE_ACTION_TYPE_ALERT:
        return Optional.empty();
      default:
        log.error("Invalid rule action type: {} for rule with rule id: {}", actionType, id);
        return Optional.empty();
    }
  }
}
