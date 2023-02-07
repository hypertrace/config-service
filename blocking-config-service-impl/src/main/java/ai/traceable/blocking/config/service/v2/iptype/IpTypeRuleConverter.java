package ai.traceable.blocking.config.service.v2.iptype;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleAggregatorBase.GenericIpTypeRuleConverter;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo;
import ai.traceable.blocking.config.service.v2.IpRange;
import ai.traceable.blocking.config.service.v2.IpType;
import ai.traceable.blocking.config.service.v2.IpTypeRule;
import ai.traceable.blocking.config.service.v2.IpV4Range;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import java.util.Map;
import java.util.stream.Collectors;

public class IpTypeRuleConverter implements GenericIpTypeRuleConverter<IpTypeRule> {
  private static final Map<IpLocationType, IpType> IP_TYPE_MAPPING =
      Map.of(
          IpLocationType.IP_LOCATION_TYPE_BOT,
          IpType.IP_TYPE_BOT,
          IpLocationType.IP_LOCATION_TYPE_ANONYMOUS_VPN,
          IpType.IP_TYPE_VPN,
          IpLocationType.IP_LOCATION_TYPE_HOSTING_PROVIDER,
          IpType.IP_TYPE_HOSTING_PROVIDER,
          IpLocationType.IP_LOCATION_TYPE_PUBLIC_PROXY,
          IpType.IP_TYPE_PROXY,
          IpLocationType.IP_LOCATION_TYPE_TOR_EXIT_NODE,
          IpType.IP_TYPE_TOR);

  @Override
  public IpTypeRule convert(IpTypeRuleInfo commonIpTypeRule) {
    return IpTypeRule.newBuilder()
        .setIpType(convert(commonIpTypeRule.getIpLocationType()))
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
        .build();
  }

  public static IpType convert(IpLocationType ipLocationType) {
    return IP_TYPE_MAPPING.get(ipLocationType);
  }
}
