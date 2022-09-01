package ai.traceable.config.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.validator.routines.InetAddressValidator;

public class IpAddressParsingUtils {
  private final InetAddressValidator ipAddressValidator;

  public IpAddressParsingUtils() {
    this.ipAddressValidator = new InetAddressValidator();
  }

  public IpParsingResults parseRawIpRange(List<String> rawIpRanges) {
    List<String> ipAddresses = new ArrayList<>();
    List<String> ipRanges = new ArrayList<>();
    rawIpRanges.forEach(
        rawIpRange -> {
          if (isValidIp(rawIpRange)) {
            ipAddresses.add(rawIpRange);
          } else if (isValidSubnet(rawIpRange)) {
            ipRanges.add(rawIpRange);
          } else {
            throw new IllegalArgumentException(
                String.format(
                    "Invalid IP range value: %s, doesn't follow CIDR format", rawIpRange));
          }
        });
    return IpParsingResults.builder()
        .ipAddresses(ipAddresses.stream().distinct().collect(Collectors.toUnmodifiableList()))
        .ipRanges(ipRanges.stream().distinct().collect(Collectors.toUnmodifiableList()))
        .build();
  }

  private boolean isValidIp(String ipAddress) {
    return ipAddressValidator.isValid(ipAddress);
  }

  private boolean isValidSubnet(String subnet) {
    // validate IP range CIDR syntax with SubnetUtils
    try {
      new SubnetUtils(subnet);
      return true;
    } catch (IllegalArgumentException iae) {
      return false;
    }
  }

  @Value
  @Builder
  public static class IpParsingResults {
    List<String> ipAddresses;
    List<String> ipRanges;
  }
}
