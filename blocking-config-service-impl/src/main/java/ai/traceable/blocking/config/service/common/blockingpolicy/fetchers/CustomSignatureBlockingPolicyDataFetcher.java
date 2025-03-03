package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.CUSTOM_SIGNATURE_ANALYTICS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.CUSTOM_SIGNATURE_EXEMPTIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.CUSTOM_SIGNATURE_VIOLATIONS;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CombinationBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CombinationBlockingDetails.Operator;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CustomSignatureBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpTypeBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.CustomSignatureRuleEffectConverter;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureInlineRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.IpType;
import ai.traceable.customsignature.config.service.v1.RegionExpression.Region;
import ai.traceable.customsignature.config.service.v1.RuleEffectWithModifications;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class CustomSignatureBlockingPolicyDataFetcher implements BlockingPolicyDataFetcherBase {
  private static final String NON_BLOCKING_RULE_INFO = "Rule has some non-blocking agent action";
  private static final List<EventType> BLOCKING_EVENT_TYPES_LIST =
      ImmutableList.of(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING, EventType.EVENT_TYPE_ALLOW);
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  CustomSignatureBlockingPolicyDataFetcher(BlockingRulesUtils blockingRulesUtils) {
    this.blockingRulesUtils = blockingRulesUtils;
  }

  @Override
  public BlockingPolicyAggregate<BlockingPolicyData> getBlockingPolicyData(
      RequestContext requestContext,
      BlockingPolicyDataFilter filter,
      BlockingRulesSupplier blockingRulesSupplier) {
    List<CustomSignatureInlineRule> ruleList =
        blockingRulesSupplier.getCustomSignatureInlineRules();
    return new BlockingPolicyAggregate<>(
        ruleList.stream()
            .map(CustomSignatureInlineRule::getRule)
            .filter(this::filterRule)
            .map(this::getBlockingDetails)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toUnmodifiableList()));
  }

  private Optional<BlockingPolicyData> getBlockingDetails(CustomSignatureRule customSignatureRule) {
    if (!blockingRulesUtils.isRuleActive(
        customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis())) {
      return Optional.empty();
    }
    Optional<BlockingPolicyData.RuleType> ruleType =
        getRuleType(customSignatureRule.getId(), customSignatureRule.getEffect().getEventType());
    Optional<BlockingPolicyDataBucket> ruleBucket =
        getRuleBucket(customSignatureRule.getEffect().getEventType());
    Optional<String> info = getInfo(customSignatureRule);
    if (ruleType.isEmpty() || ruleBucket.isEmpty() || info.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        BlockingPolicyData.builder()
            .category(Category.CUSTOM_SIGNATURE_RULE)
            .ruleType(ruleType.get())
            .bucket(ruleBucket.get())
            .info(info.get())
            .timestamp(customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis())
            .status(
                blockingRulesUtils.generateBlockingStatus(
                    customSignatureRule.getBlockingExpiryDetails().getExpiryTimestampMillis(),
                    ruleType.get()))
            .blockingDetails(buildBlockingDetails(customSignatureRule))
            .ruleId(customSignatureRule.getId())
            .action(
                CustomSignatureRuleEffectConverter.convert(
                    customSignatureRule.getEffect().getEffectsList()))
            .build());
  }

  private static BlockingDetails buildBlockingDetails(CustomSignatureRule customSignatureRule) {
    ClauseGroup clauseGroup = customSignatureRule.getDefinition().getClauseGroup();
    // We shouldn't get any customSignatureRule here that has a clause that's not
    // supported by agents - ensure this in GetCustomSignatureModsecRulesResponse
    List<BlockingDetails> blockingDetails =
        clauseGroup.getClausesList().stream()
            .map(CustomSignatureBlockingPolicyDataFetcher::buildBlockingDetails)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    boolean hasModsecConvertibleExpression =
        clauseGroup.getClausesList().stream()
            .anyMatch(
                clause ->
                    clause.hasMatchExpression()
                        || clause.hasKeyValueExpression()
                        || clause.hasCustomSecRule());
    if (hasModsecConvertibleExpression) {
      blockingDetails.add(
          CustomSignatureBlockingDetails.builder().ruleId(customSignatureRule.getId()).build());
    }
    if (blockingDetails.size() == 1) {
      return blockingDetails.get(0);
    }
    return CombinationBlockingDetails.builder()
        .operator(
            clauseGroup.getClauseOperator().equals(ClauseOperator.CLAUSE_OPERATOR_AND)
                ? Operator.AND
                : Operator.OR)
        .blockingDetailsOperands(blockingDetails)
        .build();
  }

  private static Optional<BlockingDetails> buildBlockingDetails(Clause clause) {
    switch (clause.getClauseCase()) {
      case IP_ADDRESS_EXPRESSION:
        IpBlockingDetails ipBlockingDetails =
            IpBlockingDetails.builder()
                .ipAddresses(clause.getIpAddressExpression().getIpAddressesList())
                .ipRanges(clause.getIpAddressExpression().getCidrIpRangesList())
                .build();
        if (!clause.getIpAddressExpression().getExclude()) {
          return Optional.of(ipBlockingDetails);
        }
        return Optional.of(
            CombinationBlockingDetails.builder()
                .operator(Operator.NOT)
                .blockingDetailsOperands(List.of(ipBlockingDetails))
                .build());
      case IP_TYPE_EXPRESSION:
        IpTypeBlockingDetails ipTypeBlockingDetails =
            IpTypeBlockingDetails.builder()
                .ipTypes(
                    clause.getIpTypeExpression().getIpTypesList().stream()
                        .map(CustomSignatureBlockingPolicyDataFetcher::convert)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toUnmodifiableList()))
                .build();
        if (!clause.getIpTypeExpression().getExclude()) {
          return Optional.of(ipTypeBlockingDetails);
        }
        return Optional.of(
            CombinationBlockingDetails.builder()
                .operator(Operator.NOT)
                .blockingDetailsOperands(List.of(ipTypeBlockingDetails))
                .build());
      case CUSTOM_SEC_RULE:
      case MATCH_EXPRESSION:
      case KEY_VALUE_EXPRESSION:
        // these are supported by agents, but we will add only one blocking detail per rule for
        // modsec part of the rule instead of having a blocking detail for each clause.
        return Optional.empty();
      case REGION_EXPRESSION:
        RegionBlockingDetails regionBlockingDetails =
            RegionBlockingDetails.builder()
                .regions(
                    clause.getRegionExpression().getRegionIdentifiersList().stream()
                        .map(Region::getCountryIsoCode)
                        .collect(Collectors.toUnmodifiableList()))
                .build();
        if (!clause.getRegionExpression().getExclude()) {
          return Optional.of(regionBlockingDetails);
        }
        return Optional.of(
            CombinationBlockingDetails.builder()
                .operator(Operator.NOT)
                .blockingDetailsOperands(List.of(regionBlockingDetails))
                .build());
      default:
        log.error("Received unsupported clause type: {}", clause.getClauseCase());
        return Optional.empty();
    }
  }

  private static Optional<String> getInfo(CustomSignatureRule rule) {
    switch (rule.getEffect().getEventType()) {
      case EVENT_TYPE_ALLOW:
        return Optional.of(
            ExemptionInfoEncoder.getEncodedCustomSignatureRuleExemptionInfo(
                rule.getId(), rule.getName(), rule.getEffect().getEventSeverity().name()));
      case EVENT_TYPE_DETECTION_AND_BLOCKING:
        return Optional.of(
            ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
                rule.getId(),
                rule.getName(),
                rule.getEffect().getEventSeverity().name(),
                rule.getDefinition().getLabelsMap()));
      case EVENT_TYPE_NORMAL_DETECTION:
      case EVENT_TYPE_TESTING_DETECTION:
        return Optional.of(NON_BLOCKING_RULE_INFO);
      default:
        log.info("Could not find info for rule with rule id: {}", rule.getId());
        return Optional.empty();
    }
  }

  private static Optional<BlockingPolicyDataBucket> getRuleBucket(EventType eventType) {
    switch (eventType) {
      case EVENT_TYPE_ALLOW:
        return Optional.of(CUSTOM_SIGNATURE_EXEMPTIONS);
      case EVENT_TYPE_DETECTION_AND_BLOCKING:
        return Optional.of(CUSTOM_SIGNATURE_VIOLATIONS);
      case EVENT_TYPE_NORMAL_DETECTION:
      case EVENT_TYPE_TESTING_DETECTION:
        return Optional.of(CUSTOM_SIGNATURE_ANALYTICS);
      default:
        log.info("No bucket type exist for event type : {}", eventType);
        return Optional.empty();
    }
  }

  private static Optional<BlockingPolicyData.RuleType> getRuleType(String id, EventType eventType) {
    switch (eventType) {
      case EVENT_TYPE_ALLOW:
        return Optional.of(BlockingPolicyData.RuleType.ALLOW);
      case EVENT_TYPE_DETECTION_AND_BLOCKING:
        return Optional.of(BlockingPolicyData.RuleType.BLOCK);
      case EVENT_TYPE_NORMAL_DETECTION:
      case EVENT_TYPE_TESTING_DETECTION:
        return Optional.of(RuleType.ANALYTICS);
      default:
        log.info("Invalid rule event type: {} for rule with rule id: {}", eventType, id);
        return Optional.empty();
    }
  }

  private boolean filterRule(CustomSignatureRule customSignatureRule) {
    // If the rule type is blocking filter
    if (BLOCKING_EVENT_TYPES_LIST.contains(customSignatureRule.getEffect().getEventType())) {
      return true;
    }
    // If there is some agent rule effect then filter
    return customSignatureRule.getEffect().getEffectsList().stream()
        .anyMatch(RuleEffectWithModifications::hasAgentRuleEffect);
  }

  private static IpLocationType convert(IpType ipType) {
    switch (ipType) {
      case IP_TYPE_ANONYMOUS_VPN:
        return IpLocationType.IP_LOCATION_TYPE_ANONYMOUS_VPN;
      case IP_TYPE_HOSTING_PROVIDER:
        return IpLocationType.IP_LOCATION_TYPE_HOSTING_PROVIDER;
      case IP_TYPE_PUBLIC_PROXY:
        return IpLocationType.IP_LOCATION_TYPE_PUBLIC_PROXY;
      case IP_TYPE_TOR_EXIT_NODE:
        return IpLocationType.IP_LOCATION_TYPE_TOR_EXIT_NODE;
      case IP_TYPE_BOT:
        return IpLocationType.IP_LOCATION_TYPE_BOT;
      default:
        log.info("Cannot convert ip type: {}", ipType);
        return null;
    }
  }
}
