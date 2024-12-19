package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import ai.traceable.edge.decision.config.service.v1.LogicalMatchCondition;
import ai.traceable.edge.decision.config.service.v1.LogicalMatchOperator;
import ai.traceable.edge.decision.config.service.v1.MatchCondition;
import ai.traceable.platform.traceenricher.constants.EnrichedSpanConstants;
import ai.traceable.ratelimiting.config.service.v2.IpLocationType;
import ai.traceable.ratelimiting.config.service.v2.IpLocationTypeCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RateLimitingIpTypeConditionConverter implements RateLimitingConditionConverter {

  private static final String IP_TYPE_COMPARISON_JEXL_EXP =
      "$s.getIpIntelligenceData().getTraits().getIpTypes().contains(IPType.%s)";

  @Override
  public MatchCondition buildMatchCondition(LeafCondition leafCondition) {
    IpLocationTypeCondition ipLocationTypeCondition = leafCondition.getIpLocationTypeCondition();
    List<EnrichedSpanConstants.IPType> ipTypes = getIpTypes(ipLocationTypeCondition);
    List<MatchCondition> matchConditions =
        ipTypes.stream()
            .map(ipType -> String.format(IP_TYPE_COMPARISON_JEXL_EXP, ipType.name()))
            .map(
                jexlExp ->
                    ipLocationTypeCondition.getExclude()
                        ? JexlUtils.getNotJexlExpression(jexlExp)
                        : jexlExp)
            .map(JexlUtils::getMatchCondition)
            .collect(Collectors.toUnmodifiableList());

    return MatchCondition.newBuilder()
        .setLogicalMatchCondition(
            LogicalMatchCondition.newBuilder()
                .setOperator(
                    ipLocationTypeCondition.getExclude()
                        ? LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND
                        : LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR)
                .addAllConditions(matchConditions))
        .build();
  }

  private List<EnrichedSpanConstants.IPType> getIpTypes(IpLocationTypeCondition condition) {
    return condition.getIpLocationTypesList().stream()
        .map(this::getIpType)
        .filter(Objects::nonNull)
        .collect(Collectors.toUnmodifiableList());
  }

  private EnrichedSpanConstants.IPType getIpType(IpLocationType ipLocationType) {
    switch (ipLocationType) {
      case IP_LOCATION_TYPE_ANONYMOUS_VPN:
        return EnrichedSpanConstants.IPType.IP_TYPE_ANONYMOUS_VPN;
      case IP_LOCATION_TYPE_HOSTING_PROVIDER:
        return EnrichedSpanConstants.IPType.IP_TYPE_HOSTING_PROVIDER;
      case IP_LOCATION_TYPE_PUBLIC_PROXY:
        return EnrichedSpanConstants.IPType.IP_TYPE_PUBLIC_PROXY;
      case IP_LOCATION_TYPE_TOR_EXIT_NODE:
        return EnrichedSpanConstants.IPType.IP_TYPE_TOR_EXIT_NODE;
      case IP_LOCATION_TYPE_BOT:
        return EnrichedSpanConstants.IPType.IP_TYPE_BOT;
      case IP_LOCATION_TYPE_SCANNER:
        return EnrichedSpanConstants.IPType.IP_TYPE_SCANNER;
      default:
        return null;
    }
  }

  @Override
  public LeafCondition.ConditionCase getConditionCase() {
    return LeafCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION;
  }
}
