package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.IpAsnExpression;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.processing.common.v1.IpAttributeType;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import java.util.UUID;

public class IpAsnExpressionRuleDefinitionConverter
    implements CustomSignatureRuleDefinitionConverter {

  @Override
  public CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause) {
    IpAsnExpression ipAsnExpression = clause.getIpAsnExpression();
    if (ipAsnExpression.getIpAsnRegexesList().isEmpty()) {
      throw new IllegalArgumentException("No ASN regexes present in ip asn expression");
    }

    String keyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyIpPrefix(
            IpAttributeType.IP_ATTRIBUTE_TYPE_IP_ASN);
    String joinedRegex =
        CustomSignatureRuleDefinitionConverterUtils.joinRegexes(
            ipAsnExpression.getIpAsnRegexesList());
    MatchConditionExpression conditionExpression =
        CustomSignatureRuleDefinitionConverterUtils.buildStringLikeCondition(
            keyPrefix, joinedRegex);

    if (ipAsnExpression.getExclude()) {
      conditionExpression =
          CustomSignatureRuleDefinitionConverterUtils.buildNegateExpression(conditionExpression);
    }

    String evaluationIdentifier = "ip_asn-" + UUID.randomUUID().toString().substring(0, 8);
    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, conditionExpression);
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.IP_ASN_EXPRESSION;
  }
}
