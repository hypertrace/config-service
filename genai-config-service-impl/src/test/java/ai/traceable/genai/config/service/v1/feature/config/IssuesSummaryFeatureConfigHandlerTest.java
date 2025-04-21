package ai.traceable.genai.config.service.v1.feature.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.IssuesSummaryFeatureConfig;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IssuesSummaryFeatureConfigHandlerTest {

  private IssuesSummaryFeatureConfigHandler handler;

  @BeforeEach
  void setUp() {
    handler = new IssuesSummaryFeatureConfigHandler();
  }

  @Test
  void test_getFeatureName_returnsCorrectName() {
    assertEquals("issues_summary_feature_config", handler.getFeatureName());
  }

  @Test
  void test_mergeConfigs_whenHighPriorityHasConfig_returnsHighPriorityConfig() {
    IssuesSummaryFeatureConfig highPriorityFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    IssuesSummaryFeatureConfig lowPriorityFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(false).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder().setIssuesSummaryFeatureConfig(highPriorityFeatureConfig).build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder().setIssuesSummaryFeatureConfig(lowPriorityFeatureConfig).build();

    Optional<IssuesSummaryFeatureConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
  }

  @Test
  void test_mergeConfigs_whenBothHaveConfig_returnsHighPriorityConfig() {
    IssuesSummaryFeatureConfig highPriorityFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    IssuesSummaryFeatureConfig lowPriorityFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(false).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder().setIssuesSummaryFeatureConfig(highPriorityFeatureConfig).build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder().setIssuesSummaryFeatureConfig(lowPriorityFeatureConfig).build();

    Optional<IssuesSummaryFeatureConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled()); // Should have high priority's value (true)
    assertEquals(
        highPriorityFeatureConfig, result.get()); // Should exactly match high priority config
  }

  @Test
  void test_mergeConfigs_whenHighPriorityHasDisabledConfig_returnsDisabledConfig() {
    IssuesSummaryFeatureConfig highPriorityFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(false).build();
    IssuesSummaryFeatureConfig lowPriorityFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder().setIssuesSummaryFeatureConfig(highPriorityFeatureConfig).build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder().setIssuesSummaryFeatureConfig(lowPriorityFeatureConfig).build();

    Optional<IssuesSummaryFeatureConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertFalse(result.get().getEnabled()); // Should be disabled as per high priority config
    assertEquals(
        highPriorityFeatureConfig, result.get()); // Should exactly match high priority config
  }

  @Test
  void test_mergeConfigs_whenOnlyLowPriorityHasConfig_returnsLowPriorityConfig() {
    IssuesSummaryFeatureConfig lowPriorityFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();

    GenAiConfig highPriority = GenAiConfig.getDefaultInstance();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder().setIssuesSummaryFeatureConfig(lowPriorityFeatureConfig).build();

    Optional<IssuesSummaryFeatureConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
  }

  @Test
  void test_mergeConfigs_whenNeitherHasConfig_returnsEmpty() {
    GenAiConfig highPriority = GenAiConfig.getDefaultInstance();
    GenAiConfig lowPriority = GenAiConfig.getDefaultInstance();

    Optional<IssuesSummaryFeatureConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertFalse(result.isPresent());
  }

  @Test
  void test_getFeatureConfigFromUpdate_whenUpdateHasConfig_returnsConfig() {
    IssuesSummaryFeatureConfig featureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    GenAiFeatureConfigUpdate update =
        GenAiFeatureConfigUpdate.newBuilder().setIssuesSummaryFeatureConfig(featureConfig).build();

    Optional<IssuesSummaryFeatureConfig> result = handler.getFeatureConfigFromUpdate(update);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
  }

  @Test
  void test_getFeatureConfigFromUpdate_whenUpdateHasDisabledConfig_returnsDisabledConfig() {
    IssuesSummaryFeatureConfig featureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(false).build();
    GenAiFeatureConfigUpdate update =
        GenAiFeatureConfigUpdate.newBuilder().setIssuesSummaryFeatureConfig(featureConfig).build();

    Optional<IssuesSummaryFeatureConfig> result = handler.getFeatureConfigFromUpdate(update);

    assertTrue(result.isPresent());
    assertFalse(result.get().getEnabled());
    assertEquals(featureConfig, result.get());
  }

  @Test
  void test_getFeatureConfigFromUpdate_whenUpdateDoesNotHaveConfig_returnsEmpty() {
    GenAiFeatureConfigUpdate update = GenAiFeatureConfigUpdate.getDefaultInstance();

    Optional<IssuesSummaryFeatureConfig> result = handler.getFeatureConfigFromUpdate(update);

    assertFalse(result.isPresent());
  }
}
