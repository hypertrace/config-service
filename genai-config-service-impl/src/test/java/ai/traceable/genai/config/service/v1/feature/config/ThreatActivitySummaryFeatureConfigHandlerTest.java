package ai.traceable.genai.config.service.v1.feature.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.ThreatActivitySummaryFeatureConfig;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ThreatActivitySummaryFeatureConfigHandlerTest {

  private ThreatActivitySummaryFeatureConfigHandler handler;

  @BeforeEach
  void setUp() {
    handler = new ThreatActivitySummaryFeatureConfigHandler();
  }

  @Test
  void test_getFeatureName_returnsCorrectName() {
    assertEquals("threat_activity_summary_feature_config", handler.getFeatureName());
  }

  @Test
  void test_mergeConfigs_whenHighPriorityHasConfig_returnsHighPriorityConfig() {
    ThreatActivitySummaryFeatureConfig highPriorityFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build();
    ThreatActivitySummaryFeatureConfig lowPriorityFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(false).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder()
            .setThreatActivitySummaryFeatureConfig(highPriorityFeatureConfig)
            .build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder()
            .setThreatActivitySummaryFeatureConfig(lowPriorityFeatureConfig)
            .build();

    Optional<ThreatActivitySummaryFeatureConfig> result =
        handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
  }

  @Test
  void test_mergeConfigs_whenBothHaveConfig_returnsHighPriorityConfig() {
    ThreatActivitySummaryFeatureConfig highPriorityFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build();
    ThreatActivitySummaryFeatureConfig lowPriorityFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(false).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder()
            .setThreatActivitySummaryFeatureConfig(highPriorityFeatureConfig)
            .build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder()
            .setThreatActivitySummaryFeatureConfig(lowPriorityFeatureConfig)
            .build();

    Optional<ThreatActivitySummaryFeatureConfig> result =
        handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled()); // Should have high priority's value (true)
    assertEquals(
        highPriorityFeatureConfig, result.get()); // Should exactly match high priority config
  }

  @Test
  void test_mergeConfigs_whenHighPriorityHasDisabledConfig_returnsDisabledConfig() {
    ThreatActivitySummaryFeatureConfig highPriorityFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(false).build();
    ThreatActivitySummaryFeatureConfig lowPriorityFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder()
            .setThreatActivitySummaryFeatureConfig(highPriorityFeatureConfig)
            .build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder()
            .setThreatActivitySummaryFeatureConfig(lowPriorityFeatureConfig)
            .build();

    Optional<ThreatActivitySummaryFeatureConfig> result =
        handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertFalse(result.get().getEnabled()); // Should be disabled as per high priority config
    assertEquals(
        highPriorityFeatureConfig, result.get()); // Should exactly match high priority config
  }

  @Test
  void test_mergeConfigs_whenOnlyLowPriorityHasConfig_returnsLowPriorityConfig() {
    ThreatActivitySummaryFeatureConfig lowPriorityFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build();

    GenAiConfig highPriority = GenAiConfig.getDefaultInstance();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder()
            .setThreatActivitySummaryFeatureConfig(lowPriorityFeatureConfig)
            .build();

    Optional<ThreatActivitySummaryFeatureConfig> result =
        handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
  }

  @Test
  void test_mergeConfigs_whenNeitherHasConfig_returnsEmpty() {
    GenAiConfig highPriority = GenAiConfig.getDefaultInstance();
    GenAiConfig lowPriority = GenAiConfig.getDefaultInstance();

    Optional<ThreatActivitySummaryFeatureConfig> result =
        handler.mergeConfigs(highPriority, lowPriority);

    assertFalse(result.isPresent());
  }

  @Test
  void test_getFeatureConfigFromUpdate_whenUpdateHasConfig_returnsConfig() {
    ThreatActivitySummaryFeatureConfig featureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build();
    GenAiFeatureConfigUpdate update =
        GenAiFeatureConfigUpdate.newBuilder()
            .setThreatActivitySummaryFeatureConfig(featureConfig)
            .build();

    Optional<ThreatActivitySummaryFeatureConfig> result =
        handler.getFeatureConfigFromUpdate(update);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
  }

  @Test
  void test_getFeatureConfigFromUpdate_whenUpdateHasDisabledConfig_returnsDisabledConfig() {
    ThreatActivitySummaryFeatureConfig featureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(false).build();
    GenAiFeatureConfigUpdate update =
        GenAiFeatureConfigUpdate.newBuilder()
            .setThreatActivitySummaryFeatureConfig(featureConfig)
            .build();

    Optional<ThreatActivitySummaryFeatureConfig> result =
        handler.getFeatureConfigFromUpdate(update);

    assertTrue(result.isPresent());
    assertFalse(result.get().getEnabled());
    assertEquals(featureConfig, result.get());
  }

  @Test
  void test_getFeatureConfigFromUpdate_whenUpdateDoesNotHaveConfig_returnsEmpty() {
    GenAiFeatureConfigUpdate update = GenAiFeatureConfigUpdate.getDefaultInstance();

    Optional<ThreatActivitySummaryFeatureConfig> result =
        handler.getFeatureConfigFromUpdate(update);

    assertFalse(result.isPresent());
  }
}
