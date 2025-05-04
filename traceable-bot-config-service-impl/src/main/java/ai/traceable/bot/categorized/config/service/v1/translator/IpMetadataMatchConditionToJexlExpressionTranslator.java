package ai.traceable.bot.categorized.config.service.v1.translator;

import ai.traceable.bot.categorized.config.service.v1.IpMetadataMatchConditon;
import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;

@UtilityClass
public class IpMetadataMatchConditionToJexlExpressionTranslator {

  public static String buildJexlExpression(final IpMetadataMatchConditon ipMetadataMatchCondition) {
    final String ipv4MatchCondition = getIpv4MatchCondition(ipMetadataMatchCondition);
    final String ipv6MatchCondition = getIpv6MatchCondition(ipMetadataMatchCondition);
    if (!ipv6MatchCondition.isEmpty() && !ipv4MatchCondition.isEmpty()) {
      return String.format("%s || %s", ipv4MatchCondition, ipv6MatchCondition);
    } else if (!ipv4MatchCondition.isEmpty()) {
      return ipv4MatchCondition;
    } else {
      return ipv6MatchCondition;
    }
  }

  private static String getIpv4MatchCondition(
      final IpMetadataMatchConditon ipMetadataMatchCondition) {
    return buildIpMatchConditions(
        ipMetadataMatchCondition.getIpAddressCondition().getIpv4CidrIpRangesList());
  }

  private static String getIpv6MatchCondition(
      final IpMetadataMatchConditon ipMetadataMatchCondition) {
    return buildIpMatchConditions(
        ipMetadataMatchCondition.getIpAddressCondition().getIpv6CidrIpRangesList());
  }

  private static String buildIpMatchConditions(final List<String> ipRanges) {
    return ipRanges.stream()
        .map(
            ipRange ->
                String.format("ipValidation:isIpAddressInRange('%s', $s.getIpAddress())", ipRange))
        .collect(Collectors.joining(" || "));
  }
}
