package ai.traceable.iprange.config.service.utils;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IpValidationUtilsTest {
  private IpValidationUtils ipValidationUtils;

  @BeforeEach
  void setUp() {
    ipValidationUtils = new IpValidationUtils();
  }

  @Test
  void isValidIp() {
    assertFalse(ipValidationUtils.isValidIp("1234"));
    assertFalse(ipValidationUtils.isValidIp("Apple"));
    assertTrue(ipValidationUtils.isValidIp("1.2.3.4"));
    assertFalse(ipValidationUtils.isValidIp("1:2:3:4/32"));
    assertFalse(ipValidationUtils.isValidIp("192.168.100.14/24"));
    assertFalse(ipValidationUtils.isValidIp("256:3:4:5"));
  }

  @Test
  void isValidSubnet() {
    assertFalse(ipValidationUtils.isValidSubnet("1234"));
    assertFalse(ipValidationUtils.isValidSubnet("Apple"));
    assertFalse(ipValidationUtils.isValidSubnet("1.2.3.4"));
    assertTrue(ipValidationUtils.isValidSubnet("192.168.100.14/24"));
    assertTrue(ipValidationUtils.isValidSubnet("1.2.3.4/32"));
    assertTrue(ipValidationUtils.isValidSubnet("1.2.3.4/31"));
  }
}
