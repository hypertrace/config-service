package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.IpReputationExpression;
import ai.traceable.customsignature.config.service.v1.IpReputationSeverity;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.processing.common.v1.IpAttributeType;
import ai.traceable.protection.processing.common.v1.ScoreCategoryStrict;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class IpReputationExpressionRuleDefinitionConverter
    implements CustomSignatureRuleDefinitionConverter {

  @Override
  public CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause) {
    IpReputationExpression ipReputationExpression = clause.getIpReputationExpression();

    List<String> applicableIpReputationSeverities =
        getApplicableIpReputationSeverities(ipReputationExpression.getMinIpReputationSeverity());

    String keyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyIpPrefix(
            IpAttributeType.IP_ATTRIBUTE_TYPE_IP_REPUTATION_RISK_LEVEL);

    MatchConditionExpression conditionExpression =
        CustomSignatureRuleDefinitionConverterUtils.buildStringListAnyEqualsCondition(
            keyPrefix, applicableIpReputationSeverities);

    String evaluationIdentifier = "ip_reputation-" + UUID.randomUUID().toString().substring(0, 8);
    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, conditionExpression);
  }

  private List<String> getApplicableIpReputationSeverities(
      IpReputationSeverity minIpReputationSeverity) {
    List<String> applicableIpReputationSeverities = new ArrayList<>();
    switch (minIpReputationSeverity) {
      case IP_REPUTATION_SEVERITY_LOW:
        applicableIpReputationSeverities.add(ScoreCategoryStrict.SCORE_CATEGORY_STRICT_LOW.name());
      case IP_REPUTATION_SEVERITY_MEDIUM:
        applicableIpReputationSeverities.add(
            ScoreCategoryStrict.SCORE_CATEGORY_STRICT_MEDIUM.name());
      case IP_REPUTATION_SEVERITY_HIGH:
        applicableIpReputationSeverities.add(ScoreCategoryStrict.SCORE_CATEGORY_STRICT_HIGH.name());
      case IP_REPUTATION_SEVERITY_CRITICAL:
        applicableIpReputationSeverities.add(
            ScoreCategoryStrict.SCORE_CATEGORY_STRICT_CRITICAL.name());
        break;
      default:
        throw new IllegalArgumentException(
            "Invalid ipReputationSeverity : " + minIpReputationSeverity);
    }
    return applicableIpReputationSeverities;
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.IP_REPUTATION_EXPRESSION;
  }
}
