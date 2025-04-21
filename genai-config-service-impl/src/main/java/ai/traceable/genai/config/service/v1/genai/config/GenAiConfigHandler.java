package ai.traceable.genai.config.service.v1.genai.config;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.feature.config.GenAiFeatureConfigHandler;
import jakarta.inject.Inject;
import java.util.Set;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class GenAiConfigHandler {

  private final Set<GenAiFeatureConfigHandler<?>> featureConfigHandlers;

  public GenAiConfig mergeConfigs(GenAiConfig highPriorityConfig, GenAiConfig lowPriorityConfig) {
    GenAiConfig.Builder builder = GenAiConfig.newBuilder();
    for (GenAiFeatureConfigHandler<?> featureConfigHandler : featureConfigHandlers) {
      String featureName = featureConfigHandler.getFeatureName();
      featureConfigHandler
          .mergeConfigs(highPriorityConfig, lowPriorityConfig)
          .ifPresent(
              featureConfig ->
                  builder.setField(
                      GenAiConfig.getDescriptor().findFieldByName(featureName), featureConfig));
    }
    return builder.build();
  }

  public GenAiConfig getGenAiConfigFromUpdate(GenAiFeatureConfigUpdate configUpdate) {
    GenAiConfig.Builder builder = GenAiConfig.newBuilder();
    for (GenAiFeatureConfigHandler<?> featureConfigHandler : featureConfigHandlers) {
      String featureName = featureConfigHandler.getFeatureName();
      featureConfigHandler
          .getFeatureConfigFromUpdate(configUpdate)
          .ifPresent(
              featureConfig ->
                  builder.setField(
                      GenAiConfig.getDescriptor().findFieldByName(featureName), featureConfig));
    }
    return builder.build();
  }
}
