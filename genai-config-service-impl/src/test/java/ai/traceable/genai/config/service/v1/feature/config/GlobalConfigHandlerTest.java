package ai.traceable.genai.config.service.v1.feature.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.genai.config.service.v1.FeatureLevelConfigUpdate;
import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.GlobalConfig;
import ai.traceable.genai.config.service.v1.GlobalConfigUpdate;
import ai.traceable.genai.config.service.v1.IssuesSummaryFeatureConfig;
import ai.traceable.genai.config.service.v1.UpdateGenAiConfigRequest;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GlobalConfigHandlerTest {

  private GlobalConfigHandler handler;

  @BeforeEach
  void setUp() {
    handler = new GlobalConfigHandler();
  }

  @Test
  void test_getFeatureName_returnsCorrectName() {
    assertEquals("global_config", handler.getFeatureName());
  }

  @Test
  void test_mergeConfigs_whenHighPriorityHasConfig_returnsHighPriorityConfig() {
    GlobalConfig highPriorityGlobalConfig = GlobalConfig.newBuilder().setEnabled(true).build();
    GlobalConfig lowPriorityGlobalConfig = GlobalConfig.newBuilder().setEnabled(false).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder().setGlobalConfig(highPriorityGlobalConfig).build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder().setGlobalConfig(lowPriorityGlobalConfig).build();

    Optional<GlobalConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
  }

  @Test
  void test_mergeConfigs_whenBothHaveConfig_returnsHighPriorityConfig() {
    GlobalConfig highPriorityGlobalConfig = GlobalConfig.newBuilder().setEnabled(true).build();
    GlobalConfig lowPriorityGlobalConfig = GlobalConfig.newBuilder().setEnabled(false).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder().setGlobalConfig(highPriorityGlobalConfig).build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder().setGlobalConfig(lowPriorityGlobalConfig).build();

    Optional<GlobalConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled()); // Should have high priority's value (true)
    assertEquals(
        highPriorityGlobalConfig, result.get()); // Should exactly match high priority config
  }

  @Test
  void test_mergeConfigs_whenHighPriorityHasDisabledConfig_returnsDisabledConfig() {
    GlobalConfig highPriorityGlobalConfig = GlobalConfig.newBuilder().setEnabled(false).build();
    GlobalConfig lowPriorityGlobalConfig = GlobalConfig.newBuilder().setEnabled(true).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder().setGlobalConfig(highPriorityGlobalConfig).build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder().setGlobalConfig(lowPriorityGlobalConfig).build();

    Optional<GlobalConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertFalse(result.get().getEnabled()); // Should be disabled as per high priority config
    assertEquals(
        highPriorityGlobalConfig, result.get()); // Should exactly match high priority config
  }

  @Test
  void test_mergeConfigs_whenOnlyLowPriorityHasConfig_returnsLowPriorityConfig() {
    GlobalConfig lowPriorityGlobalConfig = GlobalConfig.newBuilder().setEnabled(true).build();

    GenAiConfig highPriority = GenAiConfig.getDefaultInstance();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder().setGlobalConfig(lowPriorityGlobalConfig).build();

    Optional<GlobalConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
  }

  @Test
  void test_mergeConfigs_whenNeitherHasConfig_returnsEmpty() {
    GenAiConfig highPriority = GenAiConfig.getDefaultInstance();
    GenAiConfig lowPriority = GenAiConfig.getDefaultInstance();

    Optional<GlobalConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertFalse(result.isPresent());
  }

  @Test
  void test_getFeatureConfigFromUpdate_alwaysReturnsEmpty() {
    GenAiFeatureConfigUpdate update = GenAiFeatureConfigUpdate.getDefaultInstance();

    Optional<GlobalConfig> result = handler.getFeatureConfigFromUpdate(update);

    assertFalse(result.isPresent());
  }

  @Test
  void test_getFeatureLevelConfigFromUpdate_whenUpdateHasConfig_returnsConfig() {
    GlobalConfigUpdate globalConfigUpdate =
        GlobalConfigUpdate.newBuilder().setEnabled(true).build();
    UpdateGenAiConfigRequest update =
        UpdateGenAiConfigRequest.newBuilder().setGlobalConfigUpdate(globalConfigUpdate).build();

    Optional<GlobalConfig> result = handler.getFeatureLevelConfigFromUpdate(update);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
  }

  @Test
  void test_getFeatureLevelConfigFromUpdate_whenUpdateHasDisabledConfig_returnsDisabledConfig() {
    GlobalConfigUpdate globalConfigUpdate =
        GlobalConfigUpdate.newBuilder().setEnabled(false).build();
    UpdateGenAiConfigRequest update =
        UpdateGenAiConfigRequest.newBuilder().setGlobalConfigUpdate(globalConfigUpdate).build();

    Optional<GlobalConfig> result = handler.getFeatureLevelConfigFromUpdate(update);

    assertTrue(result.isPresent());
    assertFalse(result.get().getEnabled());
  }

  @Test
  void test_getFeatureLevelConfigFromUpdate_whenUpdateDoesNotHaveConfig_returnsEmpty() {
    UpdateGenAiConfigRequest update = UpdateGenAiConfigRequest.getDefaultInstance();

    Optional<GlobalConfig> result = handler.getFeatureLevelConfigFromUpdate(update);

    assertFalse(result.isPresent());
  }

  @Test
  void test_getFeatureLevelConfigFromUpdate_whenFeatureLevelConfigUpdatePresent_returnsEmpty() {
    IssuesSummaryFeatureConfig issuesFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    FeatureLevelConfigUpdate featureLevelUpdate =
        FeatureLevelConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfigUpdate(issuesFeatureConfig)
            .build();
    UpdateGenAiConfigRequest update =
        UpdateGenAiConfigRequest.newBuilder()
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    Optional<GlobalConfig> result = handler.getFeatureLevelConfigFromUpdate(update);

    assertFalse(result.isPresent());
  }

  @Test
  void test_getFeatureLevelConfigFromUpdate_priorityTest() {
    IssuesSummaryFeatureConfig issuesFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(false).build();
    FeatureLevelConfigUpdate featureLevelUpdate =
        FeatureLevelConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfigUpdate(issuesFeatureConfig)
            .build();

    UpdateGenAiConfigRequest update =
        UpdateGenAiConfigRequest.newBuilder()
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    Optional<GlobalConfig> result = handler.getFeatureLevelConfigFromUpdate(update);

    assertFalse(result.isPresent());
  }
}
