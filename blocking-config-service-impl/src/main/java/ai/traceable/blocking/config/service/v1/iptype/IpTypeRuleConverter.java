package ai.traceable.blocking.config.service.v1.iptype;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleAggregatorBase.GenericIpTypeRuleConverter;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo;
import ai.traceable.blocking.config.service.v1.IpRange;
import ai.traceable.blocking.config.service.v1.IpType;
import ai.traceable.blocking.config.service.v1.IpTypeRule;
import ai.traceable.blocking.config.service.v1.IpV4Range;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class IpTypeRuleConverter implements GenericIpTypeRuleConverter<IpTypeRule> {
  private static final Map<IpTypeRuleInfo.IpType, IpType> IP_TYPE_MAPPING =
      Map.of(
          IpTypeRuleInfo.IpType.BOT,
          IpType.IP_TYPE_BOT,
          IpTypeRuleInfo.IpType.ANONYMOUS_VPN,
          IpType.IP_TYPE_VPN,
          IpTypeRuleInfo.IpType.HOSTING_PROVIDER,
          IpType.IP_TYPE_HOSTING_PROVIDER,
          IpTypeRuleInfo.IpType.PUBLIC_PROXY,
          IpType.IP_TYPE_PROXY,
          IpTypeRuleInfo.IpType.TOR_EXIT_NODE,
          IpType.IP_TYPE_TOR);

  @Override
  public ImmutablePair<IpTypeRule, String> convert(IpTypeRuleInfo commonIpTypeRule) {
    return new ImmutablePair<>(
        IpTypeRule.newBuilder()
            .setIpType(convert(commonIpTypeRule.getIpType()))
            .addAllIpRanges(
                commonIpTypeRule.getIpv4Ranges().stream()
                    .map(
                        commonIpRange ->
                            IpRange.newBuilder()
                                .setIpv4Range(
                                    IpV4Range.newBuilder()
                                        .setStartIp(commonIpRange.getStart())
                                        .setEndIp(commonIpRange.getEnd()))
                                .build())
                    .collect(Collectors.toUnmodifiableList()))
            .addAllIpRanges(
                commonIpTypeRule.getIpv4Addresses().stream()
                    .map(ipAddress -> IpRange.newBuilder().setIpv4Address(ipAddress).build())
                    .collect(Collectors.toUnmodifiableList()))
            .build(),
        commonIpTypeRule.getUuid());
  }

  public static IpType convert(IpTypeRuleInfo.IpType ipLocationType) {
    return IP_TYPE_MAPPING.get(ipLocationType);
  }
}
