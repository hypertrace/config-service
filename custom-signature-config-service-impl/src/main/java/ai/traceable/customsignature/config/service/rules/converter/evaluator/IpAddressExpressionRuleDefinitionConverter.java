package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.IpAddressExpression;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.processing.common.v1.IpAttributeType;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class IpAddressExpressionRuleDefinitionConverter
    implements CustomSignatureRuleDefinitionConverter {

  @Override
  public CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause) {
    IpAddressExpression ipAddressExpression = clause.getIpAddressExpression();

    List<MatchConditionExpression> matchConditions = new ArrayList<>();

    String keyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyIpPrefix(
            IpAttributeType.IP_ATTRIBUTE_TYPE_IP_ADDRESS);

    if (!ipAddressExpression.getIpAddressesList().isEmpty()) {
      matchConditions.add(getIpAddressMatchCondition(ipAddressExpression, keyPrefix));
    }

    if (!ipAddressExpression.getCidrIpRangesList().isEmpty()) {
      matchConditions.add(getIpRangeMatchCondition(ipAddressExpression, keyPrefix));
    }

    if (matchConditions.isEmpty()) {
      throw new IllegalArgumentException(
          "No ip addresses or ranges present in ip address expression");
    }

    MatchConditionExpression finalCondition;
    if (matchConditions.size() == 1) {
      finalCondition = matchConditions.get(0);
    } else {
      finalCondition =
          CustomSignatureRuleDefinitionConverterUtils.buildOrLogicalExpression(matchConditions);
    }

    if (ipAddressExpression.getExclude()) {
      finalCondition =
          CustomSignatureRuleDefinitionConverterUtils.buildNegateExpression(finalCondition);
    }

    String evaluationIdentifier = "ip_address-" + UUID.randomUUID().toString().substring(0, 8);
    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, finalCondition);
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.IP_ADDRESS_EXPRESSION;
  }

  private MatchConditionExpression getIpRangeMatchCondition(
      IpAddressExpression ipAddressExpression, String keyPrefix) {
    return CustomSignatureRuleDefinitionConverterUtils.buildCidrMatchCondition(
        keyPrefix, ipAddressExpression.getCidrIpRangesList());
  }

  private MatchConditionExpression getIpAddressMatchCondition(
      IpAddressExpression ipAddressExpression, String keyPrefix) {
    return CustomSignatureRuleDefinitionConverterUtils.buildStringListAnyEqualsCondition(
        keyPrefix, ipAddressExpression.getIpAddressesList());
  }
}
