package org.hypertrace.config.validation;

import static java.math.BigInteger.ONE;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import java.util.Optional;

public class IpValidationUtils {
  private static final IPAddressStringParameters ADDRESS_VALIDATION_PARAMS =
      new IPAddressStringParameters.Builder()
          // Allows ipv4 joined segments like 1.2.3, 1.2, or just 1 For the case of just 1 segment
          .allow_inet_aton(false)
          // Allows an address to be specified as a single value, eg ffffffff, without the standard
          // use of segments like 1.2.3.4 or 1:2:4:3:5:6:7:8
          .allowSingleSegment(false)
          // Allows zero-length IPAddressStrings like ""
          .allowEmpty(false)
          .toParams();

  public static boolean isValidSubnet(String subnet) {
    Optional<IPAddress> maybeParsedAddress = parseAddress(subnet);
    // range will represent multiple IPs
    return maybeParsedAddress
        .filter(ipAddress -> !ipAddress.toPrefixBlock().getCount().equals(ONE))
        .isPresent();
  }

  public static boolean isValidIpAddress(String ipAddress) {
    Optional<IPAddress> maybeParsedAddress = parseAddress(ipAddress);
    return maybeParsedAddress
        .map(address -> address.toPrefixBlock().getCount().equals(ONE))
        .orElse(false);
  }

  private static Optional<IPAddress> parseAddress(String address) {
    return Optional.ofNullable(
        new IPAddressString(address, ADDRESS_VALIDATION_PARAMS).getAddress());
  }
}
