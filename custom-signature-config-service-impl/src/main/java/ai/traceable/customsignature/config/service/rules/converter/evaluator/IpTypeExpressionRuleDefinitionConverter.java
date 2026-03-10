package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.IpType;
import ai.traceable.customsignature.config.service.v1.IpTypeExpression;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.processing.common.v1.IpAttributeType;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class IpTypeExpressionRuleDefinitionConverter
    implements CustomSignatureRuleDefinitionConverter {

  @Override
  public CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause) {
    IpTypeExpression ipTypeExpression = clause.getIpTypeExpression();
    if (ipTypeExpression.getIpTypesList().isEmpty()) {
      throw new IllegalArgumentException("No ip types present in ip type expression");
    }

    String keyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyIpPrefix(
            IpAttributeType.IP_ATTRIBUTE_TYPE_IP_TYPE);
    List<String> ipTypes = new ArrayList<>();
    for (ai.traceable.protection.data.context.v1.IpType ipType : getIpTypes(ipTypeExpression)) {
      ipTypes.add(ipType.name());
    }

    MatchConditionExpression conditionExpression =
        CustomSignatureRuleDefinitionConverterUtils.buildStringListAnyEqualsCondition(
            keyPrefix, ipTypes);

    if (ipTypeExpression.getExclude()) {
      conditionExpression =
          CustomSignatureRuleDefinitionConverterUtils.buildNegateExpression(conditionExpression);
    }

    String evaluationIdentifier = "ip_type-" + UUID.randomUUID().toString().substring(0, 8);
    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, conditionExpression);
  }

  private List<ai.traceable.protection.data.context.v1.IpType> getIpTypes(
      IpTypeExpression expression) {
    List<ai.traceable.protection.data.context.v1.IpType> result = new ArrayList<>();
    for (IpType ipType : expression.getIpTypesList()) {
      result.add(getIpType(ipType));
    }
    return result;
  }

  private ai.traceable.protection.data.context.v1.IpType getIpType(IpType ipType) {
    switch (ipType) {
      case IP_TYPE_ANONYMOUS_VPN:
        return ai.traceable.protection.data.context.v1.IpType.IP_TYPE_ANONYMOUS_VPN;
      case IP_TYPE_HOSTING_PROVIDER:
        return ai.traceable.protection.data.context.v1.IpType.IP_TYPE_HOSTING_PROVIDER;
      case IP_TYPE_PUBLIC_PROXY:
        return ai.traceable.protection.data.context.v1.IpType.IP_TYPE_PROXY;
      case IP_TYPE_TOR_EXIT_NODE:
        return ai.traceable.protection.data.context.v1.IpType.IP_TYPE_TOR_EXIT_NODE;
      case IP_TYPE_BOT:
        return ai.traceable.protection.data.context.v1.IpType.IP_TYPE_BOT;
      case IP_TYPE_SCANNER:
        return ai.traceable.protection.data.context.v1.IpType.IP_TYPE_SCANNER;
      default:
        throw new IllegalArgumentException("Invalid ipType : " + ipType);
    }
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.IP_TYPE_EXPRESSION;
  }
}
