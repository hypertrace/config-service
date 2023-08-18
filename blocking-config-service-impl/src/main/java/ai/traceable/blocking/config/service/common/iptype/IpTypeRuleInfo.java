package ai.traceable.blocking.config.service.common.iptype;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class IpTypeRuleInfo {
  public IpTypeRuleInfo(IpType ipType) {
    this.ipType = ipType;
    this.ipv4Addresses = new ArrayList<>();
    this.ipv4Ranges = new ArrayList<>();
  }

  IpType ipType;
  List<IpRangeInfo> ipv4Ranges;
  List<Integer> ipv4Addresses;

  public enum IpType {
    ANONYMOUS_VPN,
    HOSTING_PROVIDER,
    PUBLIC_PROXY,
    TOR_EXIT_NODE,
    BOT;
  }

  @AllArgsConstructor
  @Getter
  @EqualsAndHashCode
  public static class IpRangeInfo {
    int start;
    int end;
  }

  @Nullable
  public static IpTypeRuleInfo.IpType convertIpType(
      ai.traceable.malicioussources.config.service.v1.IpLocationType ipLocationType) {
    switch (ipLocationType) {
      case IP_LOCATION_TYPE_BOT:
        return IpTypeRuleInfo.IpType.BOT;
      case IP_LOCATION_TYPE_ANONYMOUS_VPN:
        return IpTypeRuleInfo.IpType.ANONYMOUS_VPN;
      case IP_LOCATION_TYPE_HOSTING_PROVIDER:
        return IpTypeRuleInfo.IpType.HOSTING_PROVIDER;
      case IP_LOCATION_TYPE_PUBLIC_PROXY:
        return IpTypeRuleInfo.IpType.PUBLIC_PROXY;
      case IP_LOCATION_TYPE_TOR_EXIT_NODE:
        return IpTypeRuleInfo.IpType.TOR_EXIT_NODE;
      default:
        return null;
    }
  }

  @Nullable
  public static IpTypeRuleInfo.IpType convertIpType(
      ai.traceable.ratelimiting.config.service.v2.IpLocationType ipLocationType) {
    switch (ipLocationType) {
      case IP_LOCATION_TYPE_BOT:
        return IpTypeRuleInfo.IpType.BOT;
      case IP_LOCATION_TYPE_ANONYMOUS_VPN:
        return IpTypeRuleInfo.IpType.ANONYMOUS_VPN;
      case IP_LOCATION_TYPE_HOSTING_PROVIDER:
        return IpTypeRuleInfo.IpType.HOSTING_PROVIDER;
      case IP_LOCATION_TYPE_PUBLIC_PROXY:
        return IpTypeRuleInfo.IpType.PUBLIC_PROXY;
      case IP_LOCATION_TYPE_TOR_EXIT_NODE:
        return IpTypeRuleInfo.IpType.TOR_EXIT_NODE;
      default:
        return null;
    }
  }
}
