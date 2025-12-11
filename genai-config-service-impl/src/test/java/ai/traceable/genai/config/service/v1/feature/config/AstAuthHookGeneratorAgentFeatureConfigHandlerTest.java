package ai.traceable.genai.config.service.v1.feature.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.genai.config.service.v1.AstAuthHookGeneratorAgentFeatureConfig;
import ai.traceable.genai.config.service.v1.FeatureLevelConfigUpdate;
import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.GlobalConfig;
import ai.traceable.genai.config.service.v1.GlobalConfigUpdate;
import ai.traceable.genai.config.service.v1.UpdateGenAiConfigRequest;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AstAuthHookGeneratorAgentFeatureConfigHandlerTest {

  private AstAuthHookGeneratorAgentFeatureConfigHandler handler;

  @BeforeEach
  void setUp() {
    handler = new AstAuthHookGeneratorAgentFeatureConfigHandler();
  }

  @Test
  void test_getFeatureName_returnsCorrectName() {
    assertEquals("ast_auth_hook_generator_agent_feature_config", handler.getFeatureName());
  }

  // mergeConfigs tests
  @Test
  void test_mergeConfigs_whenHighPriorityHasConfig_returnsHighPriorityConfig() {
    AstAuthHookGeneratorAgentFeatureConfig highPriorityFeatureConfig =
        AstAuthHookGeneratorAgentFeatureConfig.newBuilder().setEnabled(true).build();
    AstAuthHookGeneratorAgentFeatureConfig lowPriorityFeatureConfig =
        AstAuthHookGeneratorAgentFeatureConfig.newBuilder().setEnabled(false).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder()
            .setAstAuthHookGeneratorAgentFeatureConfig(highPriorityFeatureConfig)
            .build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder()
            .setAstAuthHookGeneratorAgentFeatureConfig(lowPriorityFeatureConfig)
            .build();

    Optional<AstAuthHookGeneratorAgentFeatureConfig> result =
        handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
    assertEquals(highPriorityFeatureConfig, result.get());
  }

  @Test
  void test_mergeConfigs_whenOnlyHighPriorityHasGlobalConfig_returnsConfigFromGlobal() {
    GlobalConfig globalConfig = GlobalConfig.newBuilder().setEnabled(true).build();
    AstAuthHookGeneratorAgentFeatureConfig lowPriorityFeatureConfig =
        AstAuthHookGeneratorAgentFeatureConfig.newBuilder().setEnabled(false).build();

    GenAiConfig highPriority = GenAiConfig.newBuilder().setGlobalConfig(globalConfig).build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder()
            .setAstAuthHookGeneratorAgentFeatureConfig(lowPriorityFeatureConfig)
            .build();

    Optional<AstAuthHookGeneratorAgentFeatureConfig> result =
        handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
  }

  @Test
  void test_mergeConfigs_whenOnlyLowPriorityHasConfig_returnsLowPriorityConfig() {
    AstAuthHookGeneratorAgentFeatureConfig lowPriorityFeatureConfig =
        AstAuthHookGeneratorAgentFeatureConfig.newBuilder().setEnabled(true).build();

    GenAiConfig highPriority = GenAiConfig.getDefaultInstance();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder()
            .setAstAuthHookGeneratorAgentFeatureConfig(lowPriorityFeatureConfig)
            .build();

    Optional<AstAuthHookGeneratorAgentFeatureConfig> result =
        handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
    assertEquals(lowPriorityFeatureConfig, result.get());
  }

  @Test
  void test_mergeConfigs_whenNeitherHasConfig_returnsEmpty() {
    GenAiConfig highPriority = GenAiConfig.getDefaultInstance();
    GenAiConfig lowPriority = GenAiConfig.getDefaultInstance();

    Optional<AstAuthHookGeneratorAgentFeatureConfig> result =
        handler.mergeConfigs(highPriority, lowPriority);

    assertFalse(result.isPresent());
  }

  // getFeatureConfigFromUpdate tests
  @Test
  void test_getFeatureConfigFromUpdate_alwaysReturnsEmpty() {
    // Since GenAiFeatureConfigUpdate is deprecated and doesn't have the new field
    GenAiFeatureConfigUpdate update = GenAiFeatureConfigUpdate.getDefaultInstance();

    Optional<AstAuthHookGeneratorAgentFeatureConfig> result =
        handler.getFeatureConfigFromUpdate(update);

    assertFalse(result.isPresent());
  }

  // getFeatureLevelConfigFromUpdate tests
  @Test
  void test_getFeatureLevelConfigFromUpdate_whenUpdateHasGlobalConfig_returnsConfigFromGlobal() {
    GlobalConfigUpdate globalConfigUpdate =
        GlobalConfigUpdate.newBuilder().setEnabled(true).build();
    UpdateGenAiConfigRequest update =
        UpdateGenAiConfigRequest.newBuilder().setGlobalConfigUpdate(globalConfigUpdate).build();

    Optional<AstAuthHookGeneratorAgentFeatureConfig> result =
        handler.getFeatureLevelConfigFromUpdate(update);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
  }

  @Test
  void test_getFeatureLevelConfigFromUpdate_whenUpdateHasFeatureLevelConfig_returnsFeatureConfig() {
    AstAuthHookGeneratorAgentFeatureConfig featureConfig =
        AstAuthHookGeneratorAgentFeatureConfig.newBuilder().setEnabled(true).build();
    FeatureLevelConfigUpdate featureLevelUpdate =
        FeatureLevelConfigUpdate.newBuilder()
            .setAstAuthHookGeneratorAgentFeatureConfigUpdate(featureConfig)
            .build();
    UpdateGenAiConfigRequest update =
        UpdateGenAiConfigRequest.newBuilder()
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    Optional<AstAuthHookGeneratorAgentFeatureConfig> result =
        handler.getFeatureLevelConfigFromUpdate(update);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
    assertEquals(featureConfig, result.get());
  }

  @Test
  void test_getFeatureLevelConfigFromUpdate_whenUpdateHasNoRelevantConfig_returnsEmpty() {
    UpdateGenAiConfigRequest update = UpdateGenAiConfigRequest.getDefaultInstance();

    Optional<AstAuthHookGeneratorAgentFeatureConfig> result =
        handler.getFeatureLevelConfigFromUpdate(update);

    assertFalse(result.isPresent());
  }
}
