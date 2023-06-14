package org.hypertrace.config.utils.ip;

import static org.hypertrace.config.utils.ip.IpValidationUtils.getIpValidationResult;
import static org.hypertrace.config.utils.ip.TestUtils.getInValidIpAddresses;
import static org.hypertrace.config.utils.ip.TestUtils.getInValidSubnets;
import static org.hypertrace.config.utils.ip.TestUtils.getValidIpv4Addresses;
import static org.hypertrace.config.utils.ip.TestUtils.getValidIpv6Addresses;
import static org.hypertrace.config.utils.ip.TestUtils.getValidSubnets;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class IpValidationUtilsTest {
  @Test
  void testIsValidIpAddress() {
    List<String> validIpAddressList = new ArrayList<>(getValidIpv4Addresses());
    validIpAddressList.addAll(getValidIpv6Addresses());
    for (String ipAddress : validIpAddressList) {
      assertTrue(IpValidationUtils.isValidIpAddress(ipAddress));
    }

    assertFalse(IpValidationUtils.isValidIpAddress(null));
    List<String> inValidIpAddressList = new ArrayList<>(getInValidIpAddresses());
    inValidIpAddressList.addAll(getValidSubnets());
    inValidIpAddressList.addAll(getInValidSubnets());
    for (String ipAddress : inValidIpAddressList) {
      assertFalse(IpValidationUtils.isValidIpAddress(ipAddress));
    }
  }

  @Test
  void testIsValidSubnet() {
    List<String> validSubnetList = getValidSubnets();
    for (String subnet : validSubnetList) {
      assertTrue(IpValidationUtils.isValidSubnet(subnet));
    }

    assertFalse(IpValidationUtils.isValidSubnet(null));
    List<String> inValidSubnetList = new ArrayList<>(getInValidIpAddresses());
    inValidSubnetList.addAll(getInValidSubnets());
    inValidSubnetList.addAll(getValidIpv6Addresses());
    inValidSubnetList.addAll(getValidIpv4Addresses());
    for (String subnet : inValidSubnetList) {
      assertFalse(IpValidationUtils.isValidSubnet(subnet));
    }
  }

  @Test
  void testIsIpAddressInRange() {
    assertTrue(IpValidationUtils.isIpAddressInRange("192.0.2.0/24", "192.0.2.0"));
    assertTrue(IpValidationUtils.isIpAddressInRange("192.0.2.0/1", "128.0.0.0"));
    assertTrue(IpValidationUtils.isIpAddressInRange("::/3", "::"));
    assertTrue(
        IpValidationUtils.isIpAddressInRange(
            "0001:0000:0000:0000:0000:0000:0000:0000/43",
            "0001:0000:001f:ffff:ffff:ffff:ffff:ffff"));

    assertFalse(IpValidationUtils.isIpAddressInRange("10.0.0.0/24", "9.0.0.0"));
    assertFalse(IpValidationUtils.isIpAddressInRange("10.0.0.64/26", "10.0.0.0"));
    assertTrue(IpValidationUtils.isIpAddressInRange("1.2.3.4", "1.2.3.4"));
    assertTrue(IpValidationUtils.isIpAddressInRange("1::2", "1:0:0:0:0:0:0:2"));
    assertFalse(IpValidationUtils.isIpAddressInRange("1.2.2.4", "1.2.3.4"));
    assertFalse(
        IpValidationUtils.isIpAddressInRange(
            "0001:0000:0000:0000:0000:0000:0000:0000/43",
            "0000:0000:0000:0000:0000:0000:0000:0001"));
    assertFalse(IpValidationUtils.isIpAddressInRange("1.2.2.4111", "1.2.3.4"));
    assertFalse(IpValidationUtils.isIpAddressInRange("1.2.2.4", "1111.2.3.4"));
  }

  @Test
  void testIsIpAddressInList() {
    List<String> ipAddressList = List.of("1::2", "1.2.3.4", "::", "1.0.0.2");
    assertTrue(IpValidationUtils.isIpAddressInList(ipAddressList, "1:0:0:0:0:0:0:2"));
    assertTrue(IpValidationUtils.isIpAddressInList(ipAddressList, "1.2.3.4"));
    assertTrue(IpValidationUtils.isIpAddressInList(ipAddressList, "1.0.0.2"));
    assertTrue(
        IpValidationUtils.isIpAddressInList(
            ipAddressList, "0000:0000:0000:0000:0000:0000:0000:0000"));
    assertFalse(IpValidationUtils.isIpAddressInList(ipAddressList, "2.3.4.5"));
    assertFalse(IpValidationUtils.isIpAddressInList(ipAddressList, "2001:db8:0:0:1:0:0:1"));
    assertFalse(IpValidationUtils.isIpAddressInList(ipAddressList, "::1"));
    assertFalse(IpValidationUtils.isIpAddressInList(ipAddressList, "33r3"));
    assertFalse(IpValidationUtils.isIpAddressInList(ipAddressList, "11.11.111.1111"));
    assertFalse(IpValidationUtils.isIpAddressInList(ipAddressList, "1.2"));

    List<String> invalidIpAddressList = List.of("1.2.3.4", "1.2", "", "wccev");
    assertFalse(IpValidationUtils.isIpAddressInList(invalidIpAddressList, "2.2.3.4"));
    assertFalse(IpValidationUtils.isIpAddressInList(invalidIpAddressList, "2.2.3"));
  }

  @Test
  void testGetIpValidationResult() {
    // invalid input
    IpValidationUtils.IpValidationResult result = getIpValidationResult("regrhrghr");
    assertNull(result.getParsedAddress());
    assertFalse(result.isValidSubnet());
    assertFalse(result.isValidIp());
    assertFalse(result.isIpv4());
    assertFalse(result.isIpv6());
    assertFalse(result.isReservedIp());
    assertFalse(result.isExternalIp());

    // valid subnet
    result = getIpValidationResult("1.2.3.4/16");
    assertNotNull(result.getParsedAddress());
    assertTrue(result.isValidSubnet());
    assertFalse(result.isValidIp());
    assertFalse(result.isIpv4());
    assertFalse(result.isIpv6());
    assertFalse(result.isReservedIp());
    assertFalse(result.isExternalIp());

    // valid external ipv4
    result = getIpValidationResult("1.2.3.4");
    assertNotNull(result.getParsedAddress());
    assertFalse(result.isValidSubnet());
    assertTrue(result.isValidIp());
    assertTrue(result.isIpv4());
    assertFalse(result.isIpv6());
    assertFalse(result.isReservedIp());
    assertTrue(result.isExternalIp());

    // valid external ipv4 with port
    result = getIpValidationResult("1.2.3.4:3030");
    assertNotNull(result.getParsedAddress());
    assertEquals("1.2.3.4", result.getParsedAddress().toString());
    assertFalse(result.isValidSubnet());
    assertTrue(result.isValidIp());
    assertTrue(result.isIpv4());
    assertFalse(result.isIpv6());
    assertFalse(result.isReservedIp());
    assertTrue(result.isExternalIp());

    // invalid external ipv4 with port not in range
    result = getIpValidationResult("1.2.3.4:75535");
    assertNull(result.getParsedAddress());
    assertFalse(result.isValidSubnet());
    assertFalse(result.isValidIp());
    assertFalse(result.isIpv4());
    assertFalse(result.isIpv6());
    assertFalse(result.isReservedIp());
    assertFalse(result.isExternalIp());

    // valid internal ipv6
    result = getIpValidationResult("fd01:ffff::");
    assertNotNull(result.getParsedAddress());
    assertFalse(result.isValidSubnet());
    assertTrue(result.isValidIp());
    assertFalse(result.isIpv4());
    assertTrue(result.isIpv6());
    assertTrue(result.isReservedIp());
    assertFalse(result.isExternalIp());
  }

  @ParameterizedTest
  @MethodSource("providesExternalIpAddressData")
  void testIsValidExternalIpAddress(String input, boolean expected) {
    assertEquals(expected, getIpValidationResult(input).isExternalIp());
  }

  @ParameterizedTest
  @MethodSource("providesReservedIpAddressData")
  void testIsValidReservedIpAddress(String input, boolean expected) {
    assertEquals(expected, getIpValidationResult(input).isReservedIp());
  }

  private static Stream<Arguments> providesExternalIpAddressData() {
    return Stream.of(
        // Invalid address
        Arguments.of(null, false),
        Arguments.of("", false),
        Arguments.of("2.3.4", false),
        Arguments.of("2.5.5.256", false),
        Arguments.of("2.5.5.-1", false),
        Arguments.of("0000:0000:0000:0000:0000:0000:0000:0001:0000", false),
        Arguments.of("ffgf::0001:fffff", false),
        Arguments.of("fffff::", false),
        // Reserved
        Arguments.of("10.44.0.248", false),
        Arguments.of("::1", false),
        // valid external ip address
        Arguments.of("92.5.255.255", true),
        Arguments.of("f10a:1000::1", true),
        Arguments.of("1::", true));
  }

  private static Stream<Arguments> providesReservedIpAddressData() {
    return Stream.of(
        // Reserved By First Octet
        Arguments.of(null, false),
        Arguments.of("", false),
        Arguments.of("10.2.0.255", true),
        Arguments.of("10.225.0.255", true),
        Arguments.of("127.1.0.1", true),
        Arguments.of("127.225.0.1", true),

        // Reserved By 2nd Octet
        Arguments.of("172.15.255.255", false),
        Arguments.of("172.16.0.0", true),
        Arguments.of("172.31.255.254", true),
        Arguments.of("172.32.0.0", false),
        Arguments.of("192.167.100.255", false),
        Arguments.of("192.168.100.255", true),
        Arguments.of("192.169.100.255", false),

        // Valid Non-Reserved Ip Address
        Arguments.of("49.5.255.255", false),

        // Invalid Ip Address
        Arguments.of(".5.255.255", false),
        Arguments.of("1000::1", false),
        Arguments.of(null, false),
        Arguments.of("", false),

        // Reserved By First Octet fd00::/8
        Arguments.of("fcff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", false),
        Arguments.of("fd00::", true),
        Arguments.of("fd01:ffff::", true),
        Arguments.of("fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", true),
        Arguments.of("Fdff:fFff:FFff:ffff:ffff:ffff:ffff:ffff", true),
        Arguments.of("fe00::", false),
        Arguments.of("00fd::", false),
        Arguments.of("fd::", false),

        // ::1/128
        Arguments.of("::1", true),
        Arguments.of("0::1", true),
        Arguments.of("0000::1", true),
        Arguments.of("0000:0000:0000:0000:0000:0000:0000:1", true),
        Arguments.of("0000:0000:0000:0000:0000:0000:0000:0001", true),
        Arguments.of("00:00:000:0::0001", true),

        // Valid Non-Reserved Ip Address
        Arguments.of("49.5.255.255", false),
        Arguments.of("1000::1", false),

        // Invalid Ip Address
        Arguments.of(".5.255.255", false),
        Arguments.of("0000:0000:0000:0000:0000:0000:0000:0001:0000", false),
        Arguments.of("ffgf::0001:fffff", false),
        Arguments.of("fffff::", false));
  }
}
