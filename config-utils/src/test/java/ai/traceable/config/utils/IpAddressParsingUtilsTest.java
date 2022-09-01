package ai.traceable.config.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.config.utils.IpAddressParsingUtils.IpParsingResults;
import java.util.List;
import org.junit.jupiter.api.Test;

class IpAddressParsingUtilsTest {
  @Test
  void testParseRawIpRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new IpAddressParsingUtils().parseRawIpRange(List.of("1.2.3.4", "nonipstring")));
    IpParsingResults parsed =
        new IpAddressParsingUtils()
            .parseRawIpRange(List.of("127.0.0.1", "1.2.3.4/24", "1.2.3.4", "192.168.100.14/31"));
    assertEquals(List.of("127.0.0.1", "1.2.3.4"), parsed.getIpAddresses());
    assertEquals(List.of("1.2.3.4/24", "192.168.100.14/31"), parsed.getIpRanges());
  }
}
