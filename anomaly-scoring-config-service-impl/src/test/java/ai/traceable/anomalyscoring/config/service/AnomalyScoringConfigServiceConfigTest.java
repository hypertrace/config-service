package ai.traceable.anomalyscoring.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnomalyScoringConfigServiceConfigTest {
  private AnomalyScoringConfigServiceConfig config;

  @BeforeEach
  void setup() {
    this.config = new AnomalyScoringConfigServiceConfig(buildConfig());
  }

  @Test
  void testConfig() {
    assertEquals(30, config.getDefaultMediumImpactMinScore());
    assertEquals(70, config.getDefaultHighImpactMinScore());
  }

  private Config buildConfig() {
    return ConfigFactory.parseMap(
        Map.of(
            "anomaly.scoring.config.service.impact.level.score.mediumMinScore",
            30,
            "anomaly.scoring.config.service.impact.level.score.highMinScore",
            70));
  }
}
