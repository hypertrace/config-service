package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import ai.traceable.edge.decision.config.service.v1.LogicalMatchCondition;
import ai.traceable.edge.decision.config.service.v1.LogicalMatchOperator;
import ai.traceable.edge.decision.config.service.v1.MatchCondition;
import ai.traceable.ratelimiting.config.service.v2.IpConnectionType;
import ai.traceable.ratelimiting.config.service.v2.IpConnectionTypeCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingIpConnectionTypeConditionConverter
    implements RateLimitingConditionConverter {

  private static final String IP_CONNECTION_TYPE_COMPARISON_JEXL_EXP =
      "$s.getIpIntelligenceData().getTraits().getConnectionType().equals(IpConnectionType.%s)";

  @Override
  public MatchCondition buildMatchCondition(
      RequestContext requestContext, LeafCondition leafCondition) {
    IpConnectionTypeCondition ipConnectionTypeCondition =
        leafCondition.getIpConnectionTypeCondition();
    List<ai.traceable.platform.traceenricher.constants.v1.IpConnectionType> ipConnectionTypes =
        getIpConnectionTypes(ipConnectionTypeCondition);
    List<MatchCondition> matchConditions =
        ipConnectionTypes.stream()
            .map(ipType -> String.format(IP_CONNECTION_TYPE_COMPARISON_JEXL_EXP, ipType.name()))
            .map(
                jexlExp ->
                    ipConnectionTypeCondition.getExclude()
                        ? JexlUtils.getNotJexlExpression(jexlExp)
                        : jexlExp)
            .map(JexlUtils::getMatchCondition)
            .collect(Collectors.toUnmodifiableList());

    return MatchCondition.newBuilder()
        .setLogicalMatchCondition(
            LogicalMatchCondition.newBuilder()
                .setOperator(
                    ipConnectionTypeCondition.getExclude()
                        ? LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND
                        : LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR)
                .addAllConditions(matchConditions))
        .build();
  }

  private List<ai.traceable.platform.traceenricher.constants.v1.IpConnectionType>
      getIpConnectionTypes(IpConnectionTypeCondition condition) {
    return condition.getIpConnectionTypesList().stream()
        .map(this::getIpConnectionType)
        .collect(Collectors.toUnmodifiableList());
  }

  private ai.traceable.platform.traceenricher.constants.v1.IpConnectionType getIpConnectionType(
      IpConnectionType ipConnectionType) {
    switch (ipConnectionType) {
      case IP_CONNECTION_TYPE_RESIDENTIAL:
        return ai.traceable.platform.traceenricher.constants.v1.IpConnectionType
            .IP_CONNECTION_TYPE_RESIDENTIAL;
      case IP_CONNECTION_TYPE_MOBILE:
        return ai.traceable.platform.traceenricher.constants.v1.IpConnectionType
            .IP_CONNECTION_TYPE_MOBILE;
      case IP_CONNECTION_TYPE_CORPORATE:
        return ai.traceable.platform.traceenricher.constants.v1.IpConnectionType
            .IP_CONNECTION_TYPE_CORPORATE;
      case IP_CONNECTION_TYPE_DATA_CENTER:
        return ai.traceable.platform.traceenricher.constants.v1.IpConnectionType
            .IP_CONNECTION_TYPE_DATA_CENTER;
      case IP_CONNECTION_TYPE_EDUCATION:
        return ai.traceable.platform.traceenricher.constants.v1.IpConnectionType
            .IP_CONNECTION_TYPE_EDUCATION;
      default:
        throw new IllegalArgumentException("Invalid ipConnectionType : " + ipConnectionType);
    }
  }

  @Override
  public LeafCondition.ConditionCase getConditionCase() {
    return LeafCondition.ConditionCase.IP_CONNECTION_TYPE_CONDITION;
  }
}
