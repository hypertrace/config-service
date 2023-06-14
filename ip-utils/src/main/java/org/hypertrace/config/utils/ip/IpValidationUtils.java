package org.hypertrace.config.utils.ip;

import static java.math.BigInteger.ONE;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import java.util.List;
import lombok.Getter;
import org.apache.commons.validator.routines.RegexValidator;

/**
 * Using 3rd party library <a
 * href="https://github.com/seancfoley/IPAddress">seancfoley/IPAddress</a>. Validates an ipv6 based
 * on formats specified over <a
 * href="https://seancfoley.github.io/IPAddress/IPAddress/apidocs/inet/ipaddr/IPAddressString.html">here</a>.
 */
public class IpValidationUtils {
  private static final String IPV4_REGEX = "^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$";
  private static final RegexValidator IPV4_VALIDATOR = new RegexValidator(IPV4_REGEX);
  private static final int STANDARD_IPV4_LENGTH = 15;

  // modify here to exclude/include certain address formats
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

  /**
   * Reason for converting to prefix block explained over <a
   * href="https://github.com/seancfoley/IPAddress/wiki/Code-Examples-2:-Subnet-Containment,-Matching,-Comparing#containment-within-the-enclosing-cidr-prefix-block">here</a>
   */
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

  // Caller is expected to validate the arguments
  public static boolean isIpAddressInRange(String ipRange, String ipAddress) {
    try {
      IPAddress parsedAddress = parseAddress(ipAddress).toPrefixBlock();
      IPAddress parsedRange = parseAddress(ipRange).toPrefixBlock();
      return parsedRange.contains(parsedAddress);
    } catch (NullPointerException exception) {
      return false;
    }
  }

  // Caller is expected to validate the arguments
  public static boolean isIpAddressInList(List<String> ipAddresses, String ipAddress) {
    try {
      IPAddress addressToCheck = parseAddress(ipAddress).toPrefixBlock();
      return ipAddresses.stream()
          .map(address -> parseAddress(address).toPrefixBlock())
          .anyMatch(address -> address.equals(addressToCheck));
    } catch (NullPointerException exception) {
      return false;
    }
  }

  public static IpValidationResult getIpValidationResult(String address) {
    return new IpValidationResult(address);
  }

  static IPAddress parseAddress(String address) {
    return new IPAddressString(sanitiseIPAddressString(address), ADDRESS_VALIDATION_PARAMS)
        .getAddress();
  }

  static String sanitiseIPAddressString(String address) {
    if (address == null) {
      return null;
    }
    int dotIdx = address.indexOf('.');
    int colonIdx = address.indexOf(':');
    // if ip address is not a subnet try to sanitise when both `.` & `:` are present
    if (dotIdx != -1 && colonIdx != -1 && !(address.contains("/") || address.contains("*"))) {
      if (dotIdx < colonIdx) {
        // possibly a ipv4 address with port suffixed - `54.36.149.33:8080`
        try {
          int portNumber = Integer.parseInt(address.substring(colonIdx + 1));
          if (portNumber < 1 || portNumber > 65535) {
            return address;
          }
        } catch (NumberFormatException e) {
          return address;
        }
        // take ipv4 address part only in case of valid port number
        return address.substring(0, colonIdx);
      }
    }
    return address;
  }

  private static boolean isReservedIpAddress(String ipAddress) {
    if (ipAddress.length() == STANDARD_IPV4_LENGTH) {
      return isReservedIpv4Address(ipAddress);
    } else {
      return isReservedIpv6Address(ipAddress);
    }
  }

  private static boolean isReservedIpv4Address(String ipAddress) {
    String[] ipSegments = IPV4_VALIDATOR.match(ipAddress);
    int firstSegment = Integer.parseInt(ipSegments[0]);
    int secondSegment = Integer.parseInt(ipSegments[1]);
    return firstSegment == 0
        || firstSegment == 10
        || firstSegment == 127
        || (firstSegment == 172 && secondSegment >= 16 && secondSegment <= 31)
        || (firstSegment == 192 && secondSegment == 168);
  }

  private static boolean isReservedIpv6Address(String ipAddress) {
    return ipAddress.equals("0000:0000:0000:0000:0000:0000:0000:0001")
        || ipAddress.startsWith("fd");
  }

  public static class IpValidationResult {
    @Getter private IPAddress parsedAddress;
    @Getter private boolean validSubnet = false;
    @Getter private boolean validIp = false;
    @Getter private boolean ipv4 = false;
    @Getter private boolean ipv6 = false;
    @Getter private boolean reservedIp = false;
    @Getter private boolean externalIp = false;

    private IpValidationResult(String address) {
      parsedAddress = parseAddress(address);
      if (parsedAddress != null) {
        parsedAddress = parsedAddress.toPrefixBlock();
        validIp = parsedAddress.getCount().equals(ONE);
        validSubnet = !validIp;
        if (validIp) {
          ipv4 = parsedAddress.isIPv4();
          ipv6 = !ipv4;
          reservedIp = isReservedIpAddress(parsedAddress.toFullString());
          externalIp = !reservedIp;
        }
      }
    }
  }
}
