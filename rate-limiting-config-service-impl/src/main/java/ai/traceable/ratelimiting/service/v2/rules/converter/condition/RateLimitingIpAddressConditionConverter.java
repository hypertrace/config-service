package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.MatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import ai.traceable.ratelimiting.config.service.v2.IpAddressCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingIpAddressConditionConverter implements RateLimitingConditionConverter {

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

  @Override
  public MatchCondition buildMatchCondition(
      final RequestContext requestContext, LeafCondition leafCondition) {
    IpAddressCondition ipAddressCondition = leafCondition.getIpAddressCondition();
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
          "No ip addresses or ranges present in ip address consition");
    } else if (matchConditions.size() == 1) {
      return matchConditions.get(0);
    } else {
      return MatchCondition.newBuilder()
          .setLogicalMatchCondition(
              LogicalMatchCondition.newBuilder()
                  .setOperator(
                      ipAddressCondition.getExclude()
                          ? LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND
                          : LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR)
                  .addAllConditions(matchConditions))
          .build();
    }
  }

  private List<MatchCondition> getIpRangeMatchConditions(IpAddressCondition ipAddressCondition) {
    return ipAddressCondition.getCidrIpRangesList().stream()
        .map(ipRange -> String.format(IS_IP_IN_RANGE_JEXL_EXP, ipRange))
        .map(
            jexlExp ->
                ipAddressCondition.getExclude() ? JexlUtils.getNotJexlExpression(jexlExp) : jexlExp)
        .map(JexlUtils::getMatchCondition)
        .collect(Collectors.toUnmodifiableList());
  }

  private MatchCondition getIpAddressMatchCondition(IpAddressCondition ipAddressCondition) {
    ListValue.Builder ipAddressListValue = ListValue.newBuilder();
    ipAddressCondition.getIpAddressesList().stream()
        .map(ipAddress -> Value.newBuilder().setStringValue(ipAddress))
        .forEach(ipAddressListValue::addValues);
    return MatchCondition.newBuilder()
        .setStructuredMatchCondition(
            StructuredMatchCondition.newBuilder()
                .setLhs(
                    AttributeDerivationMapping.newBuilder()
                        .setName("lhs")
                        .setType(FieldType.FIELD_TYPE_STR)
                        .addRules(
                            DerivationRule.newBuilder()
                                .setTransformationConfig(
                                    DataTransformationConfig.newBuilder()
                                        .setJexlExpression(
                                            JexlExpressionConfig.newBuilder()
                                                .setJexlExpression(IP_ADDRESS_JEXL_EXP)))))
                .setBinaryOperator(
                    BinaryOperator.newBuilder()
                        .setMatchOperator(
                            ipAddressCondition.getExclude()
                                ? MatchOperator.MATCH_OPERATOR_NOT_IN
                                : MatchOperator.MATCH_OPERATOR_IN)
                        .setListValue(ipAddressListValue)))
        .build();
  }

  @Override
  public LeafCondition.ConditionCase getConditionCase() {
    return LeafCondition.ConditionCase.IP_ADDRESS_CONDITION;
  }
}
