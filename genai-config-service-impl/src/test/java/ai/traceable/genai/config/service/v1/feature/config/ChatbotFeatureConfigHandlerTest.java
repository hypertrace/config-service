package ai.traceable.genai.config.service.v1.feature.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.genai.config.service.v1.ChatbotFeatureConfig;
import ai.traceable.genai.config.service.v1.FeatureLevelConfigUpdate;
import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.UpdateGenAiConfigRequest;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ChatbotFeatureConfigHandlerTest {

  private ChatbotFeatureConfigHandler handler;

  @BeforeEach
  void setUp() {
    handler = new ChatbotFeatureConfigHandler();
  }

  @Test
  void test_getFeatureName_returnsCorrectName() {
    assertEquals("chat_bot_feature_config", handler.getFeatureName());
  }

  @Test
  void test_mergeConfigs_whenHighPriorityHasConfig_returnsHighPriorityConfig() {
    ChatbotFeatureConfig highPriorityFeatureConfig =
        ChatbotFeatureConfig.newBuilder().setEnabled(true).build();
    ChatbotFeatureConfig lowPriorityFeatureConfig =
        ChatbotFeatureConfig.newBuilder().setEnabled(false).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder().setChatBotFeatureConfig(highPriorityFeatureConfig).build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder().setChatBotFeatureConfig(lowPriorityFeatureConfig).build();

    Optional<ChatbotFeatureConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
  }

  @Test
  void test_mergeConfigs_whenBothHaveConfig_returnsHighPriorityConfig() {
    ChatbotFeatureConfig highPriorityFeatureConfig =
        ChatbotFeatureConfig.newBuilder().setEnabled(true).build();
    ChatbotFeatureConfig lowPriorityFeatureConfig =
        ChatbotFeatureConfig.newBuilder().setEnabled(false).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder().setChatBotFeatureConfig(highPriorityFeatureConfig).build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder().setChatBotFeatureConfig(lowPriorityFeatureConfig).build();

    Optional<ChatbotFeatureConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled()); // Should have high priority's value (true)
    assertEquals(
        highPriorityFeatureConfig, result.get()); // Should exactly match high priority config
  }

  @Test
  void test_mergeConfigs_whenHighPriorityHasDisabledConfig_returnsDisabledConfig() {
    ChatbotFeatureConfig highPriorityFeatureConfig =
        ChatbotFeatureConfig.newBuilder().setEnabled(false).build();
    ChatbotFeatureConfig lowPriorityFeatureConfig =
        ChatbotFeatureConfig.newBuilder().setEnabled(true).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder().setChatBotFeatureConfig(highPriorityFeatureConfig).build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder().setChatBotFeatureConfig(lowPriorityFeatureConfig).build();

    Optional<ChatbotFeatureConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertFalse(result.get().getEnabled()); // Should be disabled as per high priority config
    assertEquals(
        highPriorityFeatureConfig, result.get()); // Should exactly match high priority config
  }

  @Test
  void test_mergeConfigs_whenOnlyLowPriorityHasConfig_returnsLowPriorityConfig() {
    ChatbotFeatureConfig lowPriorityFeatureConfig =
        ChatbotFeatureConfig.newBuilder().setEnabled(true).build();

    GenAiConfig highPriority = GenAiConfig.getDefaultInstance();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder().setChatBotFeatureConfig(lowPriorityFeatureConfig).build();

    Optional<ChatbotFeatureConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
  }

  @Test
  void test_mergeConfigs_whenNeitherHasConfig_returnsEmpty() {
    GenAiConfig highPriority = GenAiConfig.getDefaultInstance();
    GenAiConfig lowPriority = GenAiConfig.getDefaultInstance();

    Optional<ChatbotFeatureConfig> result = handler.mergeConfigs(highPriority, lowPriority);

    assertFalse(result.isPresent());
  }

  @Test
  void test_getFeatureConfigFromUpdate_whenUpdateDoesNotHaveConfig_returnsEmpty() {
    GenAiFeatureConfigUpdate update = GenAiFeatureConfigUpdate.getDefaultInstance();

    Optional<ChatbotFeatureConfig> result = handler.getFeatureConfigFromUpdate(update);

    assertFalse(result.isPresent());
  }

  @Test
  void
      test_getFeatureLevelConfigFromUpdate_whenFeatureLevelConfigUpdateHasChatbotConfig_returnsConfig() {
    ChatbotFeatureConfig chatbotFeatureConfig =
        ChatbotFeatureConfig.newBuilder().setEnabled(true).build();
    FeatureLevelConfigUpdate featureLevelUpdate =
        FeatureLevelConfigUpdate.newBuilder()
            .setChatbotFeatureConfigUpdate(chatbotFeatureConfig)
            .build();
    UpdateGenAiConfigRequest update =
        UpdateGenAiConfigRequest.newBuilder()
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    Optional<ChatbotFeatureConfig> result = handler.getFeatureLevelConfigFromUpdate(update);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
    assertEquals(chatbotFeatureConfig, result.get());
  }

  @Test
  void
      test_getFeatureLevelConfigFromUpdate_whenFeatureLevelConfigUpdateHasDisabledChatbotConfig_returnsDisabledConfig() {
    ChatbotFeatureConfig chatbotFeatureConfig =
        ChatbotFeatureConfig.newBuilder().setEnabled(false).build();
    FeatureLevelConfigUpdate featureLevelUpdate =
        FeatureLevelConfigUpdate.newBuilder()
            .setChatbotFeatureConfigUpdate(chatbotFeatureConfig)
            .build();
    UpdateGenAiConfigRequest update =
        UpdateGenAiConfigRequest.newBuilder()
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    Optional<ChatbotFeatureConfig> result = handler.getFeatureLevelConfigFromUpdate(update);

    assertTrue(result.isPresent());
    assertFalse(result.get().getEnabled());
    assertEquals(chatbotFeatureConfig, result.get());
  }

  @Test
  void
      test_getFeatureLevelConfigFromUpdate_whenFeatureLevelConfigUpdateHasNoChatbotConfig_returnsEmpty() {
    FeatureLevelConfigUpdate featureLevelUpdate = FeatureLevelConfigUpdate.getDefaultInstance();
    UpdateGenAiConfigRequest update =
        UpdateGenAiConfigRequest.newBuilder()
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    Optional<ChatbotFeatureConfig> result = handler.getFeatureLevelConfigFromUpdate(update);

    assertFalse(result.isPresent());
  }

  @Test
  void test_getFeatureLevelConfigFromUpdate_priorityTest() {
    ChatbotFeatureConfig chatbotFeatureConfig =
        ChatbotFeatureConfig.newBuilder().setEnabled(false).build();
    FeatureLevelConfigUpdate featureLevelUpdate =
        FeatureLevelConfigUpdate.newBuilder()
            .setChatbotFeatureConfigUpdate(chatbotFeatureConfig)
            .build();

    UpdateGenAiConfigRequest update =
        UpdateGenAiConfigRequest.newBuilder()
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    Optional<ChatbotFeatureConfig> result = handler.getFeatureLevelConfigFromUpdate(update);

    assertTrue(result.isPresent());
    assertFalse(result.get().getEnabled());
  }

  @Test
  void test_getFeatureLevelConfigFromUpdate_priorityTest_featureLevelOverrides() {
    ChatbotFeatureConfig chatbotFeatureConfig =
        ChatbotFeatureConfig.newBuilder().setEnabled(true).build();
    FeatureLevelConfigUpdate featureLevelUpdate =
        FeatureLevelConfigUpdate.newBuilder()
            .setChatbotFeatureConfigUpdate(chatbotFeatureConfig)
            .build();

    UpdateGenAiConfigRequest update =
        UpdateGenAiConfigRequest.newBuilder()
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    Optional<ChatbotFeatureConfig> result = handler.getFeatureLevelConfigFromUpdate(update);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
  }

  @Test
  void test_getFeatureLevelConfigFromUpdate_onlyFeatureLevelUpdate_noGlobalUpdate() {
    ChatbotFeatureConfig chatbotFeatureConfig =
        ChatbotFeatureConfig.newBuilder().setEnabled(true).build();
    FeatureLevelConfigUpdate featureLevelUpdate =
        FeatureLevelConfigUpdate.newBuilder()
            .setChatbotFeatureConfigUpdate(chatbotFeatureConfig)
            .build();

    UpdateGenAiConfigRequest update =
        UpdateGenAiConfigRequest.newBuilder()
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    Optional<ChatbotFeatureConfig> result = handler.getFeatureLevelConfigFromUpdate(update);

    assertTrue(result.isPresent());
    assertTrue(result.get().getEnabled());
    assertEquals(chatbotFeatureConfig, result.get());
  }
}
