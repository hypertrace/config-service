package ai.traceable.bot.categorized.config.service.v1.translator;

import ai.traceable.bot.categorized.config.service.v1.IpMetadataMatchConditon;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;

@UtilityClass
public class IpMetadataMatchConditionToEdgeMatchConditionTranslator {

  public static MatchCondition buildMatchCondition(
      final IpMetadataMatchConditon ipMetadataMatchCondition) {
    return MatchCondition.newBuilder()
        .setLogicalMatchCondition(
            LogicalMatchCondition.newBuilder()
                .setOperator(LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR)
                .addAllConditions(getIpv4MatchCondition(ipMetadataMatchCondition))
                .addAllConditions(getIpv6MatchCondition(ipMetadataMatchCondition))
                .build())
        .build();
  }

  private static List<MatchCondition> getIpv4MatchCondition(
      final IpMetadataMatchConditon ipMetadataMatchCondition) {
    return buildIpMatchConditions(
        ipMetadataMatchCondition.getIpAddressCondition().getIpv4CidrIpRangesList());
  }

  private static List<MatchCondition> getIpv6MatchCondition(
      final IpMetadataMatchConditon ipMetadataMatchCondition) {
    return buildIpMatchConditions(
        ipMetadataMatchCondition.getIpAddressCondition().getIpv6CidrIpRangesList());
  }

  private static List<MatchCondition> buildIpMatchConditions(final List<String> ipRanges) {
    return ipRanges.stream()
        .map(
            ipRange ->
                String.format("ipValidation:isIpAddressInRange('%s', $s.getIpAddress())", ipRange))
        .map(
            jexlExpression ->
                MatchCondition.newBuilder()
                    .setGenericMatchCondition(
                        ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition
                            .newBuilder()
                            .setJexlExpression(
                                JexlExpressionConfig.newBuilder()
                                    .setJexlExpression(jexlExpression)))
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }
}
