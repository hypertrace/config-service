package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition;

import static ai.traceable.edge.decision.converter.utils.Constants.IP_TYPE_COMPARISON_JEXL_EXP;

import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpLocationType;
import ai.traceable.detection.exclusion.config.service.v1.IpLocationTypeCondition;
import ai.traceable.edge.decision.converter.utils.ConverterUtils;
import ai.traceable.edge.decision.converter.utils.JexlUtils;
import ai.traceable.platform.traceenricher.constants.EnrichedSpanConstants;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DetectionExclusionRuleIpLocationTypeConditionConverter
    implements DetectionExclusionRuleConditionConverter {

  @Override
  public MatchCondition buildMatchCondition(
      RequestContext requestContext, DetectionExclusionCondition condition) {
    IpLocationTypeCondition ipLocationTypeCondition = condition.getIpLocationTypeCondition();
    List<EnrichedSpanConstants.IPType> ipTypes = getIpTypes(ipLocationTypeCondition);
    List<MatchCondition> matchConditions =
        ipTypes.stream()
            .map(ipType -> String.format(IP_TYPE_COMPARISON_JEXL_EXP, ipType.name()))
            .map(JexlUtils::getMatchCondition)
            .collect(Collectors.toUnmodifiableList());

    return ConverterUtils.buildOrMatchConditions(matchConditions)
        .setNegate(ipLocationTypeCondition.getExclude())
        .build();
  }

  private List<EnrichedSpanConstants.IPType> getIpTypes(IpLocationTypeCondition condition) {
    return condition.getIpLocationTypesList().stream()
        .map(this::getIpType)
        .collect(Collectors.toUnmodifiableList());
  }

  private EnrichedSpanConstants.IPType getIpType(IpLocationType ipLocationType) {
    switch (ipLocationType) {
      case IP_LOCATION_TYPE_ANONYMOUS_VPN:
        return EnrichedSpanConstants.IPType.IP_TYPE_ANONYMOUS_VPN;
      case IP_LOCATION_TYPE_HOSTING_PROVIDER:
        return EnrichedSpanConstants.IPType.IP_TYPE_HOSTING_PROVIDER;
      case IP_LOCATION_TYPE_PUBLIC_PROXY:
        return EnrichedSpanConstants.IPType.IP_TYPE_PUBLIC_PROXY;
      case IP_LOCATION_TYPE_TOR_EXIT_NODE:
        return EnrichedSpanConstants.IPType.IP_TYPE_TOR_EXIT_NODE;
      case IP_LOCATION_TYPE_BOT:
        return EnrichedSpanConstants.IPType.IP_TYPE_BOT;
      case IP_LOCATION_TYPE_SCANNER:
        return EnrichedSpanConstants.IPType.IP_TYPE_SCANNER;
      default:
        throw new IllegalArgumentException("Invalid ipLocationType : " + ipLocationType);
    }
  }

  @Override
  public DetectionExclusionCondition.ConditionCase getConditionCase() {
    return DetectionExclusionCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION;
  }
}
