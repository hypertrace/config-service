package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.DATA_LOSS_PREVENTION_EXEMPTIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.DATA_LOSS_PREVENTION_VIOLATIONS;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CombinationBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CombinationBlockingDetails.Operator;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CustomSignatureBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpTypeBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Action.ActionCase;
import ai.traceable.ratelimiting.config.service.v2.Action.EventSeverity;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition.LogicalOperator;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.Condition.ConditionCase;
import ai.traceable.ratelimiting.config.service.v2.ModsecRuleIdInfo;
import ai.traceable.ratelimiting.config.service.v2.ModsecRuleIdInfo.IdType;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingModsecRule;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition.Region;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DLPBlockingPolicyDataFetcher implements BlockingPolicyDataFetcherBase {
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  DLPBlockingPolicyDataFetcher(BlockingRulesUtils blockingRulesUtils) {
    this.blockingRulesUtils = blockingRulesUtils;
  }

  @Override
  public BlockingPolicyAggregate<BlockingPolicyData> getBlockingPolicyData(
      RequestContext requestContext,
      BlockingPolicyDataFilter filter,
      BlockingRulesSupplier blockingRulesSupplier) {
    LinkedHashMap<String, List<BlockingPolicyData>> serviceScopedBlockingPolicies =
        new LinkedHashMap<>();
    blockingRulesSupplier
        .getDlpRules(new LinkedHashSet<>(filter.getServiceNames()))
        .forEach(
            (serviceName, rules) ->
                serviceScopedBlockingPolicies.put(
                    serviceName,
                    rules.stream()
                        .map(this::getBlockingDetails)
                        .flatMap(Optional::stream)
                        .collect(Collectors.toUnmodifiableList())));
    return new BlockingPolicyAggregate<>(serviceScopedBlockingPolicies);
  }

  private Optional<BlockingPolicyData> getBlockingDetails(
      RateLimitingModsecRule rateLimitingModsecRule) {
    BlockingPolicyData.RuleType ruleType =
        getRuleType(
            rateLimitingModsecRule.getId(),
            rateLimitingModsecRule
                .getData()
                .getTransactionActionConfig()
                .getAction()
                .getActionCase());
    if (ruleType == null) {
      return Optional.empty();
    }

    BlockingPolicyDataBucket ruleBucket =
        ruleType == BlockingPolicyData.RuleType.BLOCK
            ? DATA_LOSS_PREVENTION_VIOLATIONS
            : DATA_LOSS_PREVENTION_EXEMPTIONS;

    long expirationTimestamp =
        rateLimitingModsecRule.getData().getTransactionActionConfig().hasExpirationTimestampMillis()
            ? rateLimitingModsecRule
                .getData()
                .getTransactionActionConfig()
                .getExpirationTimestampMillis()
            : 0;

    return Optional.of(
        BlockingPolicyData.builder()
            .category(Category.DATA_EXFILTRATION)
            .ruleType(ruleType)
            .bucket(ruleBucket)
            .info(
                getRuleInfo(
                    ruleType,
                    rateLimitingModsecRule.getId(),
                    rateLimitingModsecRule.getData().getName(),
                    rateLimitingModsecRule.getData().getTransactionActionConfig().getAction()))
            .timestamp(expirationTimestamp)
            .status(blockingRulesUtils.generateBlockingStatus(expirationTimestamp, ruleType))
            .blockingDetails(buildBlockingDetails(rateLimitingModsecRule))
            .build());
  }

  private static BlockingDetails buildBlockingDetails(
      RateLimitingModsecRule rateLimitingModsecRule) {
    List<BlockingDetails> blockingPolicyConditionsList = new ArrayList<>();

    buildBlockingDetails(rateLimitingModsecRule.getData().getCondition())
        .ifPresent(blockingPolicyConditionsList::add);

    // Add clauses for conditions corresponding to matching done via modsec rules
    rateLimitingModsecRule.getAssociatedModsecRuleIdsList().stream()
        .map(DLPBlockingPolicyDataFetcher::buildCustomSignatureRulesMultiMatch)
        .flatMap(Optional::stream)
        .forEach(blockingPolicyConditionsList::add);

    return CombinationBlockingDetails.builder()
        .operator(Operator.AND)
        .blockingDetailsOperands(blockingPolicyConditionsList)
        .build();
  }

  private static Optional<BlockingDetails> buildBlockingDetails(Condition rateLimitingCondition) {
    if (rateLimitingCondition.getConditionCase().equals(ConditionCase.LEAF_CONDITION)) {
      return getBlockingPolicyLeafCondition(rateLimitingCondition);
    }
    List<BlockingDetails> childPolicies =
        rateLimitingCondition.getCompositeCondition().getChildrenList().stream()
            .map(DLPBlockingPolicyDataFetcher::buildBlockingDetails)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toUnmodifiableList());

    if (childPolicies.size() == 0) {
      return Optional.empty();
    } else if (childPolicies.size() == 1) {
      return Optional.of(childPolicies.get(0));
    }
    return Optional.of(
        CombinationBlockingDetails.builder()
            .operator(
                rateLimitingCondition
                        .getCompositeCondition()
                        .getOperator()
                        .equals(LogicalOperator.LOGICAL_OPERATOR_OR)
                    ? Operator.OR
                    : Operator.AND)
            .blockingDetailsOperands(childPolicies)
            .build());
  }

  private static Optional<BlockingDetails> getBlockingPolicyLeafCondition(
      Condition rateLimitingCondition) {
    switch (rateLimitingCondition.getLeafCondition().getConditionCase()) {
      case REGION_CONDITION:
        return Optional.of(
            RegionBlockingDetails.builder()
                .regions(
                    rateLimitingCondition
                        .getLeafCondition()
                        .getRegionCondition()
                        .getRegionIdentifiersList()
                        .stream()
                        .map(Region::getCountryIsoCode)
                        .collect(Collectors.toUnmodifiableList()))
                .build());
      case IP_ADDRESS_CONDITION:
        return Optional.of(
            IpBlockingDetails.builder()
                .ipAddresses(
                    rateLimitingCondition
                        .getLeafCondition()
                        .getIpAddressCondition()
                        .getIpAddressesList())
                .ipRanges(
                    rateLimitingCondition
                        .getLeafCondition()
                        .getIpAddressCondition()
                        .getCidrIpRangesList())
                .build());
      case IP_LOCATION_TYPE_CONDITION:
        return Optional.of(
            IpTypeBlockingDetails.builder()
                .ipTypes(
                    rateLimitingCondition
                        .getLeafCondition()
                        .getIpLocationTypeCondition()
                        .getIpLocationTypesList()
                        .stream()
                        .map(DLPBlockingPolicyDataFetcher::convert)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toUnmodifiableList()))
                .build());
      case SCOPE_CONDITION:
      case DATATYPE_CONDITION:
      case KEY_VALUE_CONDITION:
        // These conditions are handled by their modsec rules
        return Optional.empty();
      default:
        log.info(
            "Found a condition of type: {} in a transaction action config which cannot be converted into blocking config",
            rateLimitingCondition.getLeafCondition().getConditionCase());
        return Optional.empty();
    }
  }

  private static Optional<BlockingDetails> buildCustomSignatureRulesMultiMatch(
      ModsecRuleIdInfo modsecRuleIdInfo) {
    if (modsecRuleIdInfo.getMatchingIdsList().size() == 0) {
      return Optional.empty();
    }
    if (modsecRuleIdInfo.getMatchingIdsList().size() == 1) {
      return Optional.of(
          CustomSignatureBlockingDetails.builder()
              .ruleId(modsecRuleIdInfo.getMatchingIdsList().get(0))
              .build());
    }
    return Optional.of(
        CombinationBlockingDetails.builder()
            .operator(
                modsecRuleIdInfo.getType().equals(IdType.ID_TYPE_DATA_TYPE_CUSTOM_LOCATION)
                    ? Operator.OR
                    : Operator.AND)
            .blockingDetailsOperands(
                modsecRuleIdInfo.getMatchingIdsList().stream()
                    .map(ruleId -> CustomSignatureBlockingDetails.builder().ruleId(ruleId).build())
                    .collect(Collectors.toUnmodifiableList()))
            .build());
  }

  private static BlockingPolicyData.RuleType getRuleType(String id, ActionCase actionCase) {
    switch (actionCase) {
      case ALLOW:
        return BlockingPolicyData.RuleType.ALLOW;
      case BLOCK:
        return BlockingPolicyData.RuleType.BLOCK;
      default:
        log.info("Invalid rule event type: {} for rule with rule id: {}", actionCase, id);
        return null;
    }
  }

  private static String getRuleInfo(
      BlockingPolicyData.RuleType ruleType, String id, String name, Action action) {
    switch (ruleType) {
      case ALLOW:
        return ExemptionInfoEncoder.getEncodedDLPRuleExemptionInfo(
            id, name, EventSeverity.EVENT_SEVERITY_UNSPECIFIED.name());
      case BLOCK:
        return ViolationInfoEncoder.getEncodedDLPRuleViolationInfo(
            id, name, action.getBlock().getEventSeverity().name());
      default:
        return "";
    }
  }

  private static IpLocationType convert(
      ai.traceable.ratelimiting.config.service.v2.IpLocationType ipLocationType) {
    switch (ipLocationType) {
      case IP_LOCATION_TYPE_ANONYMOUS_VPN:
        return IpLocationType.IP_LOCATION_TYPE_ANONYMOUS_VPN;
      case IP_LOCATION_TYPE_HOSTING_PROVIDER:
        return IpLocationType.IP_LOCATION_TYPE_HOSTING_PROVIDER;
      case IP_LOCATION_TYPE_PUBLIC_PROXY:
        return IpLocationType.IP_LOCATION_TYPE_PUBLIC_PROXY;
      case IP_LOCATION_TYPE_TOR_EXIT_NODE:
        return IpLocationType.IP_LOCATION_TYPE_TOR_EXIT_NODE;
      case IP_LOCATION_TYPE_BOT:
        return IpLocationType.IP_LOCATION_TYPE_BOT;
      default:
        log.info("Cannot convert ip location type: {} for transaction access rule", ipLocationType);
        return null;
    }
  }
}
