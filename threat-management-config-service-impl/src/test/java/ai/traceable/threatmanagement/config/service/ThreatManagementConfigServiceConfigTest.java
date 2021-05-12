package ai.traceable.threatmanagement.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ThreatManagementConfigServiceConfigTest {
  private ThreatManagementConfigServiceConfig config;

  @BeforeEach
  void setup() {
    this.config = new ThreatManagementConfigServiceConfig(buildConfig());
  }

  @Test
  void testConfig() {
    assertEquals(50, config.getDefaultThreatUpperBoundMediumScore());
    assertEquals(100, config.getDefaultThreatUpperBoundHighScore());
  }

  private Config buildConfig() {
    return ConfigFactory.parseMap(
        Map.of(
            "threat.management.config.service.upper.bound.score.medium",
            50,
            "threat.management.config.service.upper.bound.score.high",
            100));
  }
}
