package ai.traceable.threatscoring.config.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultThreatScoringConfigTest {
  private final DefaultThreatScoringConfig defaultThreatScoringConfig =
      new DefaultThreatScoringConfig();

  @Test
  void test_event_confidence_mapping() {
    Assertions.assertEquals(
        425,
        defaultThreatScoringConfig
            .getDefaultConfig()
            .getConfigs()
            .getEventConfidenceScoringConfig()
            .getEventConfidenceMatrixConfig()
            .getEventConfidenceMappingCount());
  }
}
