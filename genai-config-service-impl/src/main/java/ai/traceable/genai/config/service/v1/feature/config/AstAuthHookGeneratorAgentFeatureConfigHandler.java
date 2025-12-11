package ai.traceable.genai.config.service.v1.feature.config;

import ai.traceable.genai.config.service.v1.AstAuthHookGeneratorAgentFeatureConfig;
import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.UpdateGenAiConfigRequest;
import java.util.Optional;

public class AstAuthHookGeneratorAgentFeatureConfigHandler
    implements GenAiFeatureConfigHandler<AstAuthHookGeneratorAgentFeatureConfig> {

  private static final String FEATURE_NAME = "ast_auth_hook_generator_agent_feature_config";

  @Override
  public String getFeatureName() {
    return FEATURE_NAME;
  }

  @Override
  public Optional<AstAuthHookGeneratorAgentFeatureConfig> mergeConfigs(
      GenAiConfig highPriorityConfig, GenAiConfig lowPriorityConfig) {
    if (highPriorityConfig.hasAstAuthHookGeneratorAgentFeatureConfig()) {
      return Optional.of(highPriorityConfig.getAstAuthHookGeneratorAgentFeatureConfig());
    }
    if (highPriorityConfig.hasGlobalConfig()) {
      return Optional.of(
          AstAuthHookGeneratorAgentFeatureConfig.newBuilder()
              .setEnabled(highPriorityConfig.getGlobalConfig().getEnabled())
              .build());
    }
    if (lowPriorityConfig.hasAstAuthHookGeneratorAgentFeatureConfig()) {
      return Optional.of(lowPriorityConfig.getAstAuthHookGeneratorAgentFeatureConfig());
    }
    if (lowPriorityConfig.hasGlobalConfig()) {
      return Optional.of(
          AstAuthHookGeneratorAgentFeatureConfig.newBuilder()
              .setEnabled(lowPriorityConfig.getGlobalConfig().getEnabled())
              .build());
    }
    return Optional.empty();
  }

  @Override
  public Optional<AstAuthHookGeneratorAgentFeatureConfig> getFeatureConfigFromUpdate(
      GenAiFeatureConfigUpdate featureConfigUpdate) {
    // Since GenAiFeatureConfigUpdate is deprecated and doesn't have the new field,
    // we return empty. The new updates should use FeatureLevelConfigUpdate.
    return Optional.empty();
  }

  @Override
  public Optional<AstAuthHookGeneratorAgentFeatureConfig> getFeatureLevelConfigFromUpdate(
      UpdateGenAiConfigRequest configUpdate) {
    if (configUpdate.hasGlobalConfigUpdate()) {
      return Optional.of(
          AstAuthHookGeneratorAgentFeatureConfig.newBuilder()
              .setEnabled(configUpdate.getGlobalConfigUpdate().getEnabled())
              .build());
    }
    if (configUpdate.hasFeatureLevelConfigUpdate()
        && configUpdate
            .getFeatureLevelConfigUpdate()
            .hasAstAuthHookGeneratorAgentFeatureConfigUpdate()) {
      return Optional.of(
          configUpdate
              .getFeatureLevelConfigUpdate()
              .getAstAuthHookGeneratorAgentFeatureConfigUpdate());
    }
    return Optional.empty();
  }
}
