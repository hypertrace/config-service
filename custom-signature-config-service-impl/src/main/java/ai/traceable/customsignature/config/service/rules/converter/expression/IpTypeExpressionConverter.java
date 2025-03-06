package ai.traceable.customsignature.config.service.rules.converter.expression;

import static ai.traceable.edge.decision.converter.utils.Constants.IP_TYPE_COMPARISON_JEXL_EXP;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.Clause.ClauseCase;
import ai.traceable.customsignature.config.service.v1.IpType;
import ai.traceable.customsignature.config.service.v1.IpTypeExpression;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.edge.decision.converter.utils.ConverterUtils;
import ai.traceable.edge.decision.converter.utils.JexlUtils;
import ai.traceable.platform.traceenricher.constants.EnrichedSpanConstants;
import ai.traceable.platform.traceenricher.constants.EnrichedSpanConstants.IPType;
import java.util.List;
import java.util.stream.Collectors;

public class IpTypeExpressionConverter implements CustomSignatureExpressionConverter {
  @Override
  public MatchCondition buildMatchCondition(Clause clause) {
    IpTypeExpression ipTypeExpression = clause.getIpTypeExpression();
    List<IPType> ipTypes = getIpTypes(ipTypeExpression);
    List<MatchCondition> matchConditions =
        ipTypes.stream()
            .map(ipType -> String.format(IP_TYPE_COMPARISON_JEXL_EXP, ipType.name()))
            .map(JexlUtils::getMatchCondition)
            .collect(Collectors.toUnmodifiableList());

    return ConverterUtils.buildOrMatchConditions(matchConditions)
        .setNegate(ipTypeExpression.getExclude())
        .build();
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return ClauseCase.IP_TYPE_EXPRESSION;
  }

  private List<EnrichedSpanConstants.IPType> getIpTypes(IpTypeExpression expression) {
    return expression.getIpTypesList().stream()
        .map(this::getIpType)
        .collect(Collectors.toUnmodifiableList());
  }

  private EnrichedSpanConstants.IPType getIpType(IpType ipType) {
    switch (ipType) {
      case IP_TYPE_ANONYMOUS_VPN:
        return EnrichedSpanConstants.IPType.IP_TYPE_ANONYMOUS_VPN;
      case IP_TYPE_HOSTING_PROVIDER:
        return EnrichedSpanConstants.IPType.IP_TYPE_HOSTING_PROVIDER;
      case IP_TYPE_PUBLIC_PROXY:
        return EnrichedSpanConstants.IPType.IP_TYPE_PUBLIC_PROXY;
      case IP_TYPE_TOR_EXIT_NODE:
        return EnrichedSpanConstants.IPType.IP_TYPE_TOR_EXIT_NODE;
      case IP_TYPE_BOT:
        return EnrichedSpanConstants.IPType.IP_TYPE_BOT;
      case IP_TYPE_SCANNER:
        return EnrichedSpanConstants.IPType.IP_TYPE_SCANNER;
      default:
        throw new IllegalArgumentException("Invalid ipType : " + ipType);
    }
  }
}
