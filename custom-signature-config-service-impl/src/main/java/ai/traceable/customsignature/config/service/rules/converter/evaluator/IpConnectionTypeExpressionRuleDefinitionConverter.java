package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.IpConnectionType;
import ai.traceable.customsignature.config.service.v1.IpConnectionTypeExpression;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.processing.common.v1.IpAttributeType;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class IpConnectionTypeExpressionRuleDefinitionConverter
    implements CustomSignatureRuleDefinitionConverter {

  @Override
  public CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause) {
    IpConnectionTypeExpression ipConnectionTypeExpression = clause.getIpConnectionTypeExpression();
    if (ipConnectionTypeExpression.getIpConnectionTypesList().isEmpty()) {
      throw new IllegalArgumentException(
          "No ip connection types present in ip connection type expression");
    }

    String keyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyIpPrefix(
            IpAttributeType.IP_ATTRIBUTE_TYPE_IP_CONNECTION_TYPE);

    List<String> ipConnectionTypes = getIpConnectionTypes(ipConnectionTypeExpression);

    MatchConditionExpression conditionExpression =
        CustomSignatureRuleDefinitionConverterUtils.buildStringListAnyEqualsCondition(
            keyPrefix, ipConnectionTypes);

    if (ipConnectionTypeExpression.getExclude()) {
      conditionExpression =
          CustomSignatureRuleDefinitionConverterUtils.buildNegateExpression(conditionExpression);
    }

    String evaluationIdentifier =
        "ip_connection_type-" + UUID.randomUUID().toString().substring(0, 8);
    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, conditionExpression);
  }

  private List<String> getIpConnectionTypes(IpConnectionTypeExpression expression) {
    List<String> result = new ArrayList<>();
    for (IpConnectionType ipConnectionType : expression.getIpConnectionTypesList()) {
      result.add(getIpConnectionType(ipConnectionType).name());
    }
    return result;
  }

  private ai.traceable.protection.data.context.v1.IpConnectionType getIpConnectionType(
      IpConnectionType ipConnectionType) {
    switch (ipConnectionType) {
      case IP_CONNECTION_TYPE_RESIDENTIAL:
        return ai.traceable.protection.data.context.v1.IpConnectionType
            .IP_CONNECTION_TYPE_RESIDENTIAL;
      case IP_CONNECTION_TYPE_MOBILE:
        return ai.traceable.protection.data.context.v1.IpConnectionType.IP_CONNECTION_TYPE_MOBILE;
      case IP_CONNECTION_TYPE_CORPORATE:
        return ai.traceable.protection.data.context.v1.IpConnectionType
            .IP_CONNECTION_TYPE_CORPORATE;
      case IP_CONNECTION_TYPE_DATA_CENTER:
        return ai.traceable.protection.data.context.v1.IpConnectionType
            .IP_CONNECTION_TYPE_DATA_CENTER;
      case IP_CONNECTION_TYPE_EDUCATION:
        return ai.traceable.protection.data.context.v1.IpConnectionType
            .IP_CONNECTION_TYPE_EDUCATION;
      default:
        throw new IllegalArgumentException("Invalid ipConnectionType : " + ipConnectionType);
    }
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.IP_CONNECTION_TYPE_EXPRESSION;
  }
}
