package org.hypertrace.config.utils.ip;

import static java.math.BigInteger.ONE;

import inet.ipaddr.IPAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class IpAddressParsingUtils {
  public static IpParsingResults parseRawIpRange(List<String> rawIpRanges) {
    List<String> ipAddresses = new ArrayList<>();
    List<String> ipRanges = new ArrayList<>();
    rawIpRanges.forEach(
        rawIpRange -> {
          IPAddress parsedAddress = IpValidationUtils.parseAddress(rawIpRange);
          if (parsedAddress == null) {
            throw new IllegalArgumentException(
                String.format(
                    "Invalid IP range value: %s, doesn't follow CIDR format", rawIpRange));
          }

          parsedAddress = parsedAddress.toPrefixBlock();
          if (parsedAddress.getCount().equals(ONE)) {
            ipAddresses.add(rawIpRange);
          } else {
            ipRanges.add(rawIpRange);
          }
        });

    return new IpParsingResults(
        ipAddresses.stream().distinct().collect(Collectors.toUnmodifiableList()),
        ipRanges.stream().distinct().collect(Collectors.toUnmodifiableList()));
  }

  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public static class IpParsingResults {
    @Getter List<String> ipAddresses;
    @Getter List<String> ipRanges;
  }
}
