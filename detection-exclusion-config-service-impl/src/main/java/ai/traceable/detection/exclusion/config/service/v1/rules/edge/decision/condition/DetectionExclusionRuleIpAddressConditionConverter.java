package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressCondition;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DetectionExclusionRuleIpAddressConditionConverter
    implements DetectionExclusionRuleConditionConverter {

  private static final String IP_ADDRESS_JEXL_EXP = "$s.getIpAddress()";
  private static final String IS_IP_IN_RANGE_JEXL_EXP =
      "ipValidation:isIpAddressInRange('%s', $s.getIpAddress())";
  private static final String EXTERNAL_IP_JEXL_EXP = "$s.getIpValidationResult().isExternalIp()";
  private static final String INTERNAL_IP_JEXL_EXP =
      JexlUtils.getNotJexlExpression(EXTERNAL_IP_JEXL_EXP);
  private static final MatchCondition EXTERNAL_IP_MATCH_CONDITION =
      JexlUtils.getMatchCondition(EXTERNAL_IP_JEXL_EXP);
  private static final MatchCondition INTERNAL_IP_MATCH_CONDITION =
      JexlUtils.getMatchCondition(INTERNAL_IP_JEXL_EXP);
  private static final AttributeDerivationMapping IP_ADDRESS_ATTRIBUTE =
      AttributeDerivationMapping.newBuilder()
          .setName("lhs")
          .setType(FieldType.FIELD_TYPE_STR)
          .addRules(
              DerivationRule.newBuilder()
                  .setTransformationConfig(
                      DataTransformationConfig.newBuilder()
                          .setJexlExpression(
                              JexlExpressionConfig.newBuilder()
                                  .setJexlExpression(IP_ADDRESS_JEXL_EXP))))
          .build();

  @Override
  public MatchCondition buildMatchCondition(
      RequestContext requestContext, DetectionExclusionCondition condition) {
    IpAddressCondition ipAddressCondition = condition.getIpAddressCondition();
    switch (ipAddressCondition.getIpAddressConditionType()) {
      case IP_ADDRESS_CONDITION_TYPE_ALL_EXTERNAL:
        return EXTERNAL_IP_MATCH_CONDITION;
      case IP_ADDRESS_CONDITION_TYPE_ALL_INTERNAL:
        return INTERNAL_IP_MATCH_CONDITION;
      default:
        return getIpAddressAndIpRangeMatchCondition(ipAddressCondition);
    }
  }

  private MatchCondition getIpAddressAndIpRangeMatchCondition(
      IpAddressCondition ipAddressCondition) {
    List<MatchCondition> matchConditions = new ArrayList<>();
    if (!ipAddressCondition.getIpAddressesList().isEmpty()) {
      matchConditions.add(getIpAddressMatchCondition(ipAddressCondition));
    }
    if (!ipAddressCondition.getCidrIpRangesList().isEmpty()) {
      matchConditions.addAll(getIpRangeMatchConditions(ipAddressCondition));
    }

    if (matchConditions.size() == 0) {
      throw new IllegalArgumentException(
          "No ip addresses or ranges present in ip address condition");
    } else if (matchConditions.size() == 1) {
      return matchConditions.get(0).toBuilder().setNegate(ipAddressCondition.getExclude()).build();
    } else {
      return JexlUtils.buildOrMatchConditions(matchConditions)
          .setNegate(ipAddressCondition.getExclude())
          .build();
    }
  }

  private List<MatchCondition> getIpRangeMatchConditions(IpAddressCondition ipAddressCondition) {
    return ipAddressCondition.getCidrIpRangesList().stream()
        .map(ipRange -> String.format(IS_IP_IN_RANGE_JEXL_EXP, ipRange))
        .map(JexlUtils::getMatchCondition)
        .collect(Collectors.toUnmodifiableList());
  }

  private MatchCondition getIpAddressMatchCondition(IpAddressCondition ipAddressCondition) {
    return JexlUtils.buildInOperatorMatchCondition(
            IP_ADDRESS_ATTRIBUTE, ipAddressCondition.getIpAddressesList())
        .build();
  }

  @Override
  public DetectionExclusionCondition.ConditionCase getConditionCase() {
    return DetectionExclusionCondition.ConditionCase.IP_ADDRESS_CONDITION;
  }
}
