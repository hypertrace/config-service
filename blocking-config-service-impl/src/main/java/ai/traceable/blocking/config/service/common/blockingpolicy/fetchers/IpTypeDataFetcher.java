package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Status;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.iptype.BlockingIpTypesClient;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.google.inject.Inject;
import com.google.protobuf.util.Timestamps;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class IpTypeDataFetcher {
  private final BlockingIpTypesClient blockingIpTypesClient;
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  public IpTypeDataFetcher(
      BlockingIpTypesClient blockingIpTypesClient, BlockingRulesUtils blockingRulesUtils) {
    this.blockingIpTypesClient = blockingIpTypesClient;
    this.blockingRulesUtils = blockingRulesUtils;
  }

  public List<BlockingPolicyData> getIpTypeViolations(
      RequestContext requestContext, Optional<String> environmentId) {
    List<MaliciousSourcesRule> rulesList =
        blockingIpTypesClient.fetchMaliciousSourceRules(requestContext, environmentId);

    return rulesList.stream()
        .map(this::generateBlockingDetails)
        .filter(Objects::nonNull)
        .collect(Collectors.toUnmodifiableList());
  }

  private BlockingPolicyData generateBlockingDetails(MaliciousSourcesRule maliciousSourcesRule) {
    long expirationTimestampMillis =
        Timestamps.toMillis(
            maliciousSourcesRule
                .getRuleInfo()
                .getRuleAction()
                .getExpirationDetails()
                .getExpirationTimestamp());

    List<IpLocationType> ipTypes = BlockingIpTypesClient.getBlockingIpTypes(maliciousSourcesRule);
    // Filter rules
    if (ipTypes.isEmpty() || !blockingRulesUtils.isRuleActive(expirationTimestampMillis)) {
      return null;
    }

    return BlockingPolicyData.builder()
        .category(Category.IP_TYPE_RULE)
        .ruleType(RuleType.BLOCK)
        .info(
            ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
                maliciousSourcesRule.getId(),
                maliciousSourcesRule.getRuleInfo().getName(),
                maliciousSourcesRule.getRuleInfo().getRuleAction().getEventSeverity().name(),
                Optional.empty(),
                maliciousSourcesRule.getRuleInfo().getConditionsList().stream()
                    .map(MaliciousSourcesRuleCondition::getConditionCase)
                    .collect(Collectors.toUnmodifiableList())))
        .timestamp(expirationTimestampMillis)
        .status(Status.DENIED)
        .ipTypes(ipTypes)
        .build();
  }
}
