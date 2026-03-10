package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.IpAbuseVelocity;
import ai.traceable.customsignature.config.service.v1.IpAbuseVelocityExpression;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.processing.common.v1.IpAttributeType;
import ai.traceable.protection.processing.common.v1.ScoreCategoryStandard;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class IpAbuseVelocityExpressionRuleDefinitionConverter
    implements CustomSignatureRuleDefinitionConverter {

  @Override
  public CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause) {
    IpAbuseVelocityExpression ipAbuseVelocityExpression = clause.getIpAbuseVelocityExpression();

    String keyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyIpPrefix(
            IpAttributeType.IP_ATTRIBUTE_TYPE_IP_ABUSE_VELOCITY);

    List<String> applicableVelocities =
        applicableIpAbuseVelocities(ipAbuseVelocityExpression.getMinIpAbuseVelocity());

    MatchConditionExpression finalCondition =
        CustomSignatureRuleDefinitionConverterUtils.buildStringListAnyEqualsCondition(
            keyPrefix, applicableVelocities);

    String evaluationIdentifier =
        "ip_abuse_velocity-" + UUID.randomUUID().toString().substring(0, 8);

    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, finalCondition);
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.IP_ABUSE_VELOCITY_EXPRESSION;
  }

  private List<String> applicableIpAbuseVelocities(IpAbuseVelocity minIpAbuseVelocity) {
    List<String> applicableIpAbuseVelocities = new ArrayList<>();
    switch (minIpAbuseVelocity) {
      case IP_ABUSE_VELOCITY_LOW:
        applicableIpAbuseVelocities.add(ScoreCategoryStandard.SCORE_CATEGORY_STANDARD_LOW.name());
      case IP_ABUSE_VELOCITY_MEDIUM:
        applicableIpAbuseVelocities.add(
            ScoreCategoryStandard.SCORE_CATEGORY_STANDARD_MEDIUM.name());
      case IP_ABUSE_VELOCITY_HIGH:
        applicableIpAbuseVelocities.add(ScoreCategoryStandard.SCORE_CATEGORY_STANDARD_HIGH.name());
        break;
      default:
        throw new IllegalArgumentException("Invalid ipAbuseVelocity : " + minIpAbuseVelocity);
    }
    return applicableIpAbuseVelocities;
  }
}
