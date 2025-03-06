package ai.traceable.customsignature.config.service.rules.converter.expression;

import static ai.traceable.edge.decision.converter.utils.Constants.EXTERNAL_IP_JEXL_EXP;
import static ai.traceable.edge.decision.converter.utils.Constants.INTERNAL_IP_JEXL_EXP;
import static ai.traceable.edge.decision.converter.utils.Constants.IS_IP_IN_RANGE_JEXL_EXP;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.IP_ADDRESS_ATTRIBUTE;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.Clause.ClauseCase;
import ai.traceable.customsignature.config.service.v1.IpAddressExpression;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.edge.decision.converter.utils.ConverterUtils;
import ai.traceable.edge.decision.converter.utils.JexlUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class IpAddressExpressionConverter implements CustomSignatureExpressionConverter {

  @Override
  public MatchCondition buildMatchCondition(Clause clause) {
    IpAddressExpression ipAddressExpression = clause.getIpAddressExpression();
    switch (ipAddressExpression.getIpAddressExpressionType()) {
      case IP_ADDRESS_EXPRESSION_TYPE_ALL_EXTERNAL:
        return JexlUtils.getMatchCondition(EXTERNAL_IP_JEXL_EXP);
      case IP_ADDRESS_EXPRESSION_TYPE_ALL_INTERNAL:
        return JexlUtils.getMatchCondition(INTERNAL_IP_JEXL_EXP);
      default:
        return getIpAddressAndIpRangeMatchCondition(ipAddressExpression);
    }
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return ClauseCase.IP_ADDRESS_EXPRESSION;
  }

  private MatchCondition getIpAddressAndIpRangeMatchCondition(
      IpAddressExpression ipAddressExpression) {
    List<MatchCondition> matchConditions = new ArrayList<>();
    if (!ipAddressExpression.getIpAddressesList().isEmpty()) {
      matchConditions.add(getIpAddressMatchCondition(ipAddressExpression));
    }
    if (!ipAddressExpression.getCidrIpRangesList().isEmpty()) {
      matchConditions.addAll(getIpRangeMatchConditions(ipAddressExpression));
    }
    if (matchConditions.isEmpty()) {
      throw new IllegalArgumentException(
          "No ip addresses or ranges present in ip address expression");
    } else if (matchConditions.size() == 1) {
      return matchConditions.get(0).toBuilder().setNegate(ipAddressExpression.getExclude()).build();
    } else {
      return ConverterUtils.buildOrMatchConditions(matchConditions)
          .setNegate(ipAddressExpression.getExclude())
          .build();
    }
  }

  private List<MatchCondition> getIpRangeMatchConditions(IpAddressExpression ipAddressExpression) {
    return ipAddressExpression.getCidrIpRangesList().stream()
        .map(ipRange -> String.format(IS_IP_IN_RANGE_JEXL_EXP, ipRange))
        .map(JexlUtils::getMatchCondition)
        .collect(Collectors.toUnmodifiableList());
  }

  private MatchCondition getIpAddressMatchCondition(IpAddressExpression ipAddressExpression) {
    return ConverterUtils.buildInOperatorMatchCondition(
            IP_ADDRESS_ATTRIBUTE, ipAddressExpression.getIpAddressesList())
        .build();
  }
}
