package org.hypertrace.config.utils.ip;

import java.util.List;

public class TestUtils {
  static List<String> getValidIpv4Addresses() {
    return List.of("1.2.3.4", "123.54.1.56", "255.255.255.255", "0.0.0.0", "1.234.255.11");
  }

  static List<String> getValidIpv6Addresses() {
    return List.of(
        "1::2",
        "::1",
        "1:001:001:01:1:0001:001:01",
        "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
        "1:1:1:1:1:1:1.2.3.4",
        "2001:0db8::0001:0000",
        "2001:db8:0:0:1:0:0:1",
        "::",
        "::ffff:192.0.2.128",
        "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        "FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF",
        "ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255",
        "0001:0000:0000:0000:0000:0000:0000:0002");
  }

  static List<String> getInValidIpAddresses() {
    return List.of(
        "rgthgtht",
        "1,2.3.4,5.6",
        "@#$%^",
        "1234.qw.2w",
        "0.0.0.0:1.2.3.4",
        "1.1:1.2.3.4",
        "1.2.3.656",
        "1.1.1.256",
        "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffg",
        "ffff:ffreeeff:ffff:ffff:ffff:ffff:ffff:fffg",
        "ffff:ffreeeff:ffff:gff:ffff:ffff:ffff:fffg",
        "256.3.23.543",
        "1.2.#.4",
        "1.1.1.1111",
        "1.2.3.656.1.1.1.fffg",
        "",
        "   ",
        "1.2",
        "1.2.3",
        "34",
        "*" // because ambiguous between ipv4 and ipv6
        );
  }

  static List<String> getValidSubnets() {
    return List.of(
        "1.2.3.4/0",
        "0.0.0.0/0",
        "*.*",
        "1.2.3.4/16",
        "*:*",
        "/64",
        "1.*.2-3.4",
        "2002:0000:0000:1234:0000:0000:0000:0000/64",
        "1.2.0.0/16",
        "1.2.0.0/255.255.0.0",
        "1.2.*.*",
        "1.2.0-255.0-255",
        "2001:db8::/32",
        "::ffff:1.2.0.0/112",
        "1.*.2-3.4/255.255.255.0",
        "1:1:1:1:1:1:1.2.3.4/3");
  }

  static List<String> getInValidSubnets() {
    return List.of("1.2.3.4/76", "1.2/1", "1/1", "1.2.3/1");
  }
}
