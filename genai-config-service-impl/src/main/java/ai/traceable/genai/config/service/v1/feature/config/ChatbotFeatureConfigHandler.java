package ai.traceable.genai.config.service.v1.feature.config;

import ai.traceable.genai.config.service.v1.ChatbotFeatureConfig;
import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.UpdateGenAiConfigRequest;
import java.util.Optional;

public class ChatbotFeatureConfigHandler
    implements GenAiFeatureConfigHandler<ChatbotFeatureConfig> {

  private static final String FEATURE_NAME = "chat_bot_feature_config";

  @Override
  public String getFeatureName() {
    return FEATURE_NAME;
  }

  @Override
  public Optional<ChatbotFeatureConfig> mergeConfigs(
      GenAiConfig highPriorityConfig, GenAiConfig lowPriorityConfig) {
    if (highPriorityConfig.hasChatBotFeatureConfig()) {
      return Optional.of(highPriorityConfig.getChatBotFeatureConfig());
    }
    if (highPriorityConfig.hasGlobalConfig()) {
      return Optional.of(
          ChatbotFeatureConfig.newBuilder()
              .setEnabled(highPriorityConfig.getGlobalConfig().getEnabled())
              .build());
    }
    if (lowPriorityConfig.hasChatBotFeatureConfig()) {
      return Optional.of(lowPriorityConfig.getChatBotFeatureConfig());
    }
    if (lowPriorityConfig.hasGlobalConfig()) {
      return Optional.of(
          ChatbotFeatureConfig.newBuilder()
              .setEnabled(lowPriorityConfig.getGlobalConfig().getEnabled())
              .build());
    }
    return Optional.empty();
  }

  @Override
  public Optional<ChatbotFeatureConfig> getFeatureConfigFromUpdate(
      GenAiFeatureConfigUpdate featureConfigUpdate) {
    return Optional.empty();
  }

  @Override
  public Optional<ChatbotFeatureConfig> getFeatureLevelConfigFromUpdate(
      UpdateGenAiConfigRequest configUpdate) {
    if (configUpdate.hasGlobalConfigUpdate()) {
      return Optional.of(
          ChatbotFeatureConfig.newBuilder()
              .setEnabled(configUpdate.getGlobalConfigUpdate().getEnabled())
              .build());
    }
    if (configUpdate.hasFeatureLevelConfigUpdate()
        && configUpdate.getFeatureLevelConfigUpdate().hasChatbotFeatureConfigUpdate()) {
      return Optional.of(
          configUpdate.getFeatureLevelConfigUpdate().getChatbotFeatureConfigUpdate());
    }
    return Optional.empty();
  }
}
