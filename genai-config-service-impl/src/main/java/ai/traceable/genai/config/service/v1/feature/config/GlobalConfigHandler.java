package ai.traceable.genai.config.service.v1.feature.config;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.GlobalConfig;
import ai.traceable.genai.config.service.v1.UpdateGenAiConfigRequest;
import java.util.Optional;

public class GlobalConfigHandler implements GenAiFeatureConfigHandler<GlobalConfig> {

  private static final String FEATURE_NAME = "global_config";

  @Override
  public String getFeatureName() {
    return FEATURE_NAME;
  }

  @Override
  public Optional<GlobalConfig> mergeConfigs(
      GenAiConfig highPriorityConfig, GenAiConfig lowPriorityConfig) {
    if (highPriorityConfig.hasGlobalConfig()) {
      return Optional.of(highPriorityConfig.getGlobalConfig());
    }
    if (lowPriorityConfig.hasGlobalConfig()) {
      return Optional.of(lowPriorityConfig.getGlobalConfig());
    }
    return Optional.empty();
  }

  @Override
  public Optional<GlobalConfig> getFeatureConfigFromUpdate(
      GenAiFeatureConfigUpdate featureConfigUpdate) {
    return Optional.empty();
  }

  @Override
  public Optional<GlobalConfig> getFeatureLevelConfigFromUpdate(
      UpdateGenAiConfigRequest configUpdate) {
    if (configUpdate.hasGlobalConfigUpdate()) {
      return Optional.of(
          GlobalConfig.newBuilder()
              .setEnabled(configUpdate.getGlobalConfigUpdate().getEnabled())
              .build());
    }
    return Optional.empty();
  }
}
