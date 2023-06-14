package org.hypertrace.config.validation;

import static java.math.BigInteger.ONE;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;

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
    IPAddress parsedAddress = parseAddress(subnet);
    if (parsedAddress == null) {
      return false;
    }
    return !parsedAddress
        .toPrefixBlock()
        .getCount()
        .equals(ONE); // range will represent multiple IPs
  }

  public static boolean isValidIpAddress(String ipAddress) {
    IPAddress parsedAddress = parseAddress(ipAddress);
    if (parsedAddress == null) {
      return false;
    }
    return parsedAddress.toPrefixBlock().getCount().equals(ONE);
  }

  static IPAddress parseAddress(String address) {
    return new IPAddressString(address, ADDRESS_VALIDATION_PARAMS).getAddress();
  }
}
