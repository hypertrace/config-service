package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_DENIED;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.iptype.BlockingIpTypesClient;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.IpType;
import ai.traceable.blocking.config.service.v1.IpTypeDetails;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
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

  public List<BlockingDetails> getIpTypeViolations(
      RequestContext requestContext, Optional<String> environmentId) {
    List<MaliciousSourcesRule> rulesList =
        blockingIpTypesClient.fetchMaliciousSourceRules(requestContext, environmentId);

    return rulesList.stream()
        .map(this::generateBlockingDetails)
        .filter(Objects::nonNull)
        .collect(Collectors.toUnmodifiableList());
  }

  private BlockingDetails generateBlockingDetails(MaliciousSourcesRule maliciousSourcesRule) {
    long expirationTimestampMillis =
        Timestamps.toMillis(
            maliciousSourcesRule
                .getRuleInfo()
                .getRuleAction()
                .getExpirationDetails()
                .getExpirationTimestamp());

    List<IpType> ipTypes = BlockingIpTypesClient.getBlockingIpTypes(maliciousSourcesRule);

    // Filter rules
    if (ipTypes.isEmpty() || !blockingRulesUtils.isRuleActive(expirationTimestampMillis)) {
      return null;
    }

    return BlockingDetails.newBuilder()
        .setCategory(BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE)
        .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK)
        .setInfo(
            ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
                maliciousSourcesRule.getId(),
                maliciousSourcesRule.getRuleInfo().getName(),
                maliciousSourcesRule.getRuleInfo().getRuleAction().getEventSeverity().name(),
                Optional.empty()))
        .setExpirationTimestamp(expirationTimestampMillis)
        .setStatus(BLOCKING_STATUS_DENIED)
        .setIpTypeDetails(IpTypeDetails.newBuilder().addAllIpTypes(ipTypes))
        .build();
  }
}
