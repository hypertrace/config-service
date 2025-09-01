package ai.traceable.genai.config.service.v1.feature.config;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.UpdateGenAiConfigRequest;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Optional;

public interface GenAiFeatureConfigHandler<M extends GeneratedMessageV3> {

  String getFeatureName();

  Optional<M> mergeConfigs(GenAiConfig highPriorityConfig, GenAiConfig lowPriorityConfig);

  Optional<M> getFeatureConfigFromUpdate(GenAiFeatureConfigUpdate featureConfigUpdate);

  Optional<M> getFeatureLevelConfigFromUpdate(UpdateGenAiConfigRequest configUpdate);
}
