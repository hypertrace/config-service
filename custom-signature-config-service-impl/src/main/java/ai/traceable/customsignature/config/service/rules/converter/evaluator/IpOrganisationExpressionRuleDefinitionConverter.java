package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.IpOrganisationExpression;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.processing.common.v1.IpAttributeType;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import java.util.UUID;

public class IpOrganisationExpressionRuleDefinitionConverter
    implements CustomSignatureRuleDefinitionConverter {

  @Override
  public CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause) {
    IpOrganisationExpression ipOrganisationExpression = clause.getIpOrganisationExpression();
    if (ipOrganisationExpression.getIpOrganisationRegexesList().isEmpty()) {
      throw new IllegalArgumentException(
          "No organisation regexes present in ip organisation expression");
    }

    String keyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyIpPrefix(
            IpAttributeType.IP_ATTRIBUTE_TYPE_IP_ORG);
    String joinedRegex =
        CustomSignatureRuleDefinitionConverterUtils.joinRegexes(
            ipOrganisationExpression.getIpOrganisationRegexesList());
    MatchConditionExpression conditionExpression =
        CustomSignatureRuleDefinitionConverterUtils.buildStringLikeCondition(
            keyPrefix, joinedRegex);

    if (ipOrganisationExpression.getExclude()) {
      conditionExpression =
          CustomSignatureRuleDefinitionConverterUtils.buildNegateExpression(conditionExpression);
    }

    String evaluationIdentifier = "ip_org-" + UUID.randomUUID().toString().substring(0, 8);
    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, conditionExpression);
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.IP_ORGANISATION_EXPRESSION;
  }
}
