package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import ai.traceable.edge.decision.config.service.v1.LogicalMatchCondition;
import ai.traceable.edge.decision.config.service.v1.LogicalMatchOperator;
import ai.traceable.edge.decision.config.service.v1.MatchCondition;
import ai.traceable.ratelimiting.config.service.v2.IpAbuseVelocity;
import ai.traceable.ratelimiting.config.service.v2.IpAbuseVelocityCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RateLimitingIpAbuseVelocityConditionConverter
    implements RateLimitingConditionConverter {

  private static final String IP_ABUSE_VELOCITY_COMPARISON_JEXL_EXP =
      "$s.getIpIntelligenceData().getTraits().getAbuseVelocity().equals(IpAbuseVelocity.%s)";

  @Override
  public MatchCondition buildMatchCondition(LeafCondition leafCondition) {
    IpAbuseVelocityCondition ipAbuseVelocityCondition = leafCondition.getIpAbuseVelocityCondition();
    List<ai.traceable.platform.traceenricher.constants.v1.IpAbuseVelocity>
        applicableIpAbuseVelocities =
            applicableIpAbuseVelocities(ipAbuseVelocityCondition.getMinIpAbuseVelocity());
    List<MatchCondition> matchConditions =
        applicableIpAbuseVelocities.stream()
            .map(
                ipAbuseVelocity ->
                    String.format(IP_ABUSE_VELOCITY_COMPARISON_JEXL_EXP, ipAbuseVelocity.name()))
            .map(JexlUtils::getMatchCondition)
            .collect(Collectors.toUnmodifiableList());
    return MatchCondition.newBuilder()
        .setLogicalMatchCondition(
            LogicalMatchCondition.newBuilder()
                .setOperator(LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR)
                .addAllConditions(matchConditions))
        .build();
  }

  private List<ai.traceable.platform.traceenricher.constants.v1.IpAbuseVelocity>
      applicableIpAbuseVelocities(IpAbuseVelocity minIpAbuseVelocity) {
    List<ai.traceable.platform.traceenricher.constants.v1.IpAbuseVelocity>
        applicableIpAbuseVelocities = new ArrayList<>();
    switch (minIpAbuseVelocity) {
      case IP_ABUSE_VELOCITY_LOW:
        applicableIpAbuseVelocities.add(
            ai.traceable.platform.traceenricher.constants.v1.IpAbuseVelocity.IP_ABUSE_VELOCITY_LOW);
      case IP_ABUSE_VELOCITY_MEDIUM:
        applicableIpAbuseVelocities.add(
            ai.traceable.platform.traceenricher.constants.v1.IpAbuseVelocity
                .IP_ABUSE_VELOCITY_MEDIUM);
      case IP_ABUSE_VELOCITY_HIGH:
        applicableIpAbuseVelocities.add(
            ai.traceable.platform.traceenricher.constants.v1.IpAbuseVelocity
                .IP_ABUSE_VELOCITY_HIGH);
      default:
        return applicableIpAbuseVelocities;
    }
  }

  @Override
  public LeafCondition.ConditionCase getConditionCase() {
    return LeafCondition.ConditionCase.IP_ABUSE_VELOCITY_CONDITION;
  }
}
