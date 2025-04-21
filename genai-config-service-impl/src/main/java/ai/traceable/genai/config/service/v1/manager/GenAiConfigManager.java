package ai.traceable.genai.config.service.v1.manager;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.GenAiScope;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface GenAiConfigManager {

  GenAiConfig getGenAiConfig(RequestContext requestContext, GenAiScope genaAIConfigScope);

  GenAiConfig updateGenAiConfig(
      RequestContext requestContext, GenAiScope scope, GenAiFeatureConfigUpdate update);
}
