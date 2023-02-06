package ai.traceable.blocking.config.service.v1.iptype;

import ai.traceable.blocking.config.service.v1.IpType;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import java.util.Map;

public final class IpTypeConverter {
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

  public static IpType getIpType(IpLocationType ipLocationType) {
    return IP_TYPE_MAPPING.get(ipLocationType);
  }
}
