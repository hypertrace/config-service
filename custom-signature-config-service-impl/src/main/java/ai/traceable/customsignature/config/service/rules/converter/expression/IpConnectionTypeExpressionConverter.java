package ai.traceable.customsignature.config.service.rules.converter.expression;

import static ai.traceable.edge.decision.converter.utils.Constants.IP_CONNECTION_TYPE_COMPARISON_JEXL_EXP;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.Clause.ClauseCase;
import ai.traceable.customsignature.config.service.v1.IpConnectionType;
import ai.traceable.customsignature.config.service.v1.IpConnectionTypeExpression;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.edge.decision.converter.utils.ConverterUtils;
import ai.traceable.edge.decision.converter.utils.JexlUtils;
import java.util.List;
import java.util.stream.Collectors;

public class IpConnectionTypeExpressionConverter implements CustomSignatureExpressionConverter {
  @Override
  public MatchCondition buildMatchCondition(Clause clause) {
    IpConnectionTypeExpression ipConnectionTypeExpression = clause.getIpConnectionTypeExpression();
    List<ai.traceable.platform.traceenricher.constants.v1.IpConnectionType> ipConnectionTypes =
        getIpConnectionTypes(ipConnectionTypeExpression);
    List<MatchCondition> matchConditions =
        ipConnectionTypes.stream()
            .map(ipType -> String.format(IP_CONNECTION_TYPE_COMPARISON_JEXL_EXP, ipType.name()))
            .map(JexlUtils::getMatchCondition)
            .collect(Collectors.toUnmodifiableList());

    return ConverterUtils.buildOrMatchConditions(matchConditions)
        .setNegate(ipConnectionTypeExpression.getExclude())
        .build();
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return ClauseCase.IP_CONNECTION_TYPE_EXPRESSION;
  }

  private List<ai.traceable.platform.traceenricher.constants.v1.IpConnectionType>
      getIpConnectionTypes(IpConnectionTypeExpression expression) {
    return expression.getIpConnectionTypesList().stream()
        .map(this::getIpConnectionType)
        .collect(Collectors.toUnmodifiableList());
  }

  private ai.traceable.platform.traceenricher.constants.v1.IpConnectionType getIpConnectionType(
      IpConnectionType ipConnectionType) {
    switch (ipConnectionType) {
      case IP_CONNECTION_TYPE_RESIDENTIAL:
        return ai.traceable.platform.traceenricher.constants.v1.IpConnectionType
            .IP_CONNECTION_TYPE_RESIDENTIAL;
      case IP_CONNECTION_TYPE_MOBILE:
        return ai.traceable.platform.traceenricher.constants.v1.IpConnectionType
            .IP_CONNECTION_TYPE_MOBILE;
      case IP_CONNECTION_TYPE_CORPORATE:
        return ai.traceable.platform.traceenricher.constants.v1.IpConnectionType
            .IP_CONNECTION_TYPE_CORPORATE;
      case IP_CONNECTION_TYPE_DATA_CENTER:
        return ai.traceable.platform.traceenricher.constants.v1.IpConnectionType
            .IP_CONNECTION_TYPE_DATA_CENTER;
      case IP_CONNECTION_TYPE_EDUCATION:
        return ai.traceable.platform.traceenricher.constants.v1.IpConnectionType
            .IP_CONNECTION_TYPE_EDUCATION;
      default:
        throw new IllegalArgumentException("Invalid ipConnectionType : " + ipConnectionType);
    }
  }
}
