package ai.traceable.blocking.config.service.common.iptype;

import ai.traceable.config.utils.UuidGenerator;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@ToString
public class IpTypeRuleInfo {
  private IpTypeRuleInfo(IpType ipType, List<Integer> ipv4Addresses, List<IpRangeInfo> ipv4Ranges) {
    this.ipType = ipType;
    this.ipv4Addresses = ImmutableList.copyOf(ipv4Addresses);
    this.ipv4Ranges = ImmutableList.copyOf(ipv4Ranges);
    this.uuid = new UuidGenerator().generateId(String.valueOf(this));
  }

  private final IpType ipType;
  private final List<Integer> ipv4Addresses;
  private final List<IpRangeInfo> ipv4Ranges;
  private final String uuid;

  public static IpTypeRuleInfoBuilder builder() {
    return new IpTypeRuleInfoBuilder();
  }

  public enum IpType {
    ANONYMOUS_VPN,
    HOSTING_PROVIDER,
    PUBLIC_PROXY,
    TOR_EXIT_NODE,
    BOT
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

  public static class IpTypeRuleInfoBuilder {

    private IpType ipType;
    private ArrayList<Integer> ipv4Addresses;
    private ArrayList<IpRangeInfo> ipv4Ranges;

    private IpTypeRuleInfoBuilder() {}

    public IpTypeRuleInfoBuilder ipType(IpType ipType) {
      this.ipType = ipType;
      return this;
    }

    public IpTypeRuleInfoBuilder ipv4Address(Integer ipv4Address) {
      if (this.ipv4Addresses == null) {
        this.ipv4Addresses = new ArrayList<>();
      }
      this.ipv4Addresses.add(ipv4Address);
      return this;
    }

    public IpTypeRuleInfoBuilder ipv4Addresses(Collection<? extends Integer> ipv4Addresses) {
      if (this.ipv4Addresses == null) {
        this.ipv4Addresses = new ArrayList<Integer>();
      }
      this.ipv4Addresses.addAll(ipv4Addresses);
      return this;
    }

    public IpTypeRuleInfoBuilder clearIpv4Addresses() {
      if (this.ipv4Addresses != null) {
        this.ipv4Addresses.clear();
      }
      return this;
    }

    public IpTypeRuleInfoBuilder ipv4Range(IpRangeInfo ipv4Range) {
      if (this.ipv4Ranges == null) {
        this.ipv4Ranges = new ArrayList<>();
      }
      this.ipv4Ranges.add(ipv4Range);
      return this;
    }

    public IpTypeRuleInfoBuilder ipv4Ranges(Collection<? extends IpRangeInfo> ipv4Ranges) {
      if (this.ipv4Ranges == null) {
        this.ipv4Ranges = new ArrayList<>();
      }
      this.ipv4Ranges.addAll(ipv4Ranges);
      return this;
    }

    public IpTypeRuleInfo build() {
      List<Integer> ipv4Addresses;
      switch (this.ipv4Addresses == null ? 0 : this.ipv4Addresses.size()) {
        case 0:
          ipv4Addresses = java.util.Collections.emptyList();
          break;
        case 1:
          ipv4Addresses = java.util.Collections.singletonList(this.ipv4Addresses.get(0));
          break;
        default:
          ipv4Addresses =
              java.util.Collections.unmodifiableList(new ArrayList<>(this.ipv4Addresses));
      }
      List<IpRangeInfo> ipv4Ranges;
      switch (this.ipv4Ranges == null ? 0 : this.ipv4Ranges.size()) {
        case 0:
          ipv4Ranges = java.util.Collections.emptyList();
          break;
        case 1:
          ipv4Ranges = java.util.Collections.singletonList(this.ipv4Ranges.get(0));
          break;
        default:
          ipv4Ranges = java.util.Collections.unmodifiableList(new ArrayList<>(this.ipv4Ranges));
      }

      return new IpTypeRuleInfo(ipType, ipv4Addresses, ipv4Ranges);
    }

    public String toString() {
      return "IpTypeRuleInfo.IpTypeRuleInfoBuilder(ipType="
          + this.ipType
          + ", ipv4Addresses="
          + this.ipv4Addresses
          + ", ipv4Ranges="
          + this.ipv4Ranges
          + ")";
    }
  }
}
