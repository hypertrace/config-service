package ai.traceable.customsignature.config.service.rules.converter.expression;

import static ai.traceable.edge.decision.converter.utils.Constants.IP_ABUSE_VELOCITY_COMPARISON_JEXL_EXP;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.Clause.ClauseCase;
import ai.traceable.customsignature.config.service.v1.IpAbuseVelocity;
import ai.traceable.customsignature.config.service.v1.IpAbuseVelocityExpression;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.edge.decision.converter.utils.ConverterUtils;
import ai.traceable.edge.decision.converter.utils.JexlUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class IpAbuseVelocityExpressionConverter implements CustomSignatureExpressionConverter {

  @Override
  public MatchCondition buildMatchCondition(Clause clause) {
    IpAbuseVelocityExpression ipAbuseVelocityExpression = clause.getIpAbuseVelocityExpression();
    List<ai.traceable.platform.traceenricher.constants.v1.IpAbuseVelocity>
        applicableIpAbuseVelocities =
            applicableIpAbuseVelocities(ipAbuseVelocityExpression.getMinIpAbuseVelocity());
    List<MatchCondition> childConditions =
        applicableIpAbuseVelocities.stream()
            .map(
                ipAbuseVelocity ->
                    String.format(IP_ABUSE_VELOCITY_COMPARISON_JEXL_EXP, ipAbuseVelocity.name()))
            .map(JexlUtils::getMatchCondition)
            .collect(Collectors.toUnmodifiableList());
    return ConverterUtils.buildOrMatchConditions(childConditions).build();
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return ClauseCase.IP_ABUSE_VELOCITY_EXPRESSION;
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
        break;
      default:
        throw new IllegalArgumentException("Invalid ipAbuseVelocity : " + minIpAbuseVelocity);
    }
    return applicableIpAbuseVelocities;
  }
}
