package org.hypertrace.config.utils.ip;

import static org.hypertrace.config.utils.ip.IpAddressParsingUtils.parseRawIpRange;
import static org.hypertrace.config.utils.ip.TestUtils.getInValidIpAddresses;
import static org.hypertrace.config.utils.ip.TestUtils.getInValidSubnets;
import static org.hypertrace.config.utils.ip.TestUtils.getValidIpv4Addresses;
import static org.hypertrace.config.utils.ip.TestUtils.getValidIpv6Addresses;
import static org.hypertrace.config.utils.ip.TestUtils.getValidSubnets;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class IpAddressParsingUtilsTest {
  @Test
  void testParseRawIpRange() {
    List<String> invalidAddresses = new ArrayList<>(getInValidIpAddresses());
    invalidAddresses.addAll(getInValidSubnets());

    invalidAddresses.forEach(
        address ->
            assertThrows(IllegalArgumentException.class, () -> parseRawIpRange(List.of(address))));

    List<String> validIpAddresses = new ArrayList<>(getValidIpv4Addresses());
    validIpAddresses.addAll(getValidIpv6Addresses());
    List<String> validIpRanges = getValidSubnets();
    List<String> validAddress = new ArrayList<>(validIpAddresses);
    validAddress.addAll(validIpRanges);

    IpAddressParsingUtils.IpParsingResults parsed = parseRawIpRange(validAddress);
    assertEquals(validIpAddresses, parsed.getIpAddresses());
    assertEquals(validIpRanges, parsed.getIpRanges());
  }
}
