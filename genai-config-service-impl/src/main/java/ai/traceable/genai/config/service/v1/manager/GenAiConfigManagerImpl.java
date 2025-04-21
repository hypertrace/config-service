package ai.traceable.genai.config.service.v1.manager;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.GenAiScope;
import ai.traceable.genai.config.service.v1.genai.config.DefaultGenAiConfigProvider;
import ai.traceable.genai.config.service.v1.genai.config.GenAiConfigHandler;
import ai.traceable.genai.config.service.v1.store.GenAiConfigStore;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class GenAiConfigManagerImpl implements GenAiConfigManager {

  private final DefaultGenAiConfigProvider defaultGenAiConfigProvider;
  private final GenAiConfigHandler genAiConfigHandler;
  private final GenAiConfigStore genAiConfigStore;

  @Override
  public GenAiConfig getGenAiConfig(RequestContext requestContext, GenAiScope genaAIConfigScope) {
    GenAiConfig globalDefaultConfig = getMergedGlobalAndDefaultConfig(requestContext);
    if (genaAIConfigScope.equals(GenAiScope.getDefaultInstance())) {
      return buildScopedGenAiConfig(globalDefaultConfig, genaAIConfigScope);
    }
    Optional<GenAiConfig> configFromStoreOptional =
        genAiConfigStore.getGenAiConfigFromStore(requestContext, genaAIConfigScope);
    if (configFromStoreOptional.isEmpty()) {
      return buildScopedGenAiConfig(globalDefaultConfig, genaAIConfigScope);
    }
    GenAiConfig mergedConfig =
        genAiConfigHandler.mergeConfigs(configFromStoreOptional.get(), globalDefaultConfig);
    return buildScopedGenAiConfig(mergedConfig, genaAIConfigScope);
  }

  @Override
  public GenAiConfig updateGenAiConfig(
      RequestContext requestContext, GenAiScope scope, GenAiFeatureConfigUpdate update) {
    GenAiConfig genAiConfigToUpdate = genAiConfigHandler.getGenAiConfigFromUpdate(update);
    Optional<GenAiConfig> configFromStoreOptional =
        genAiConfigStore.getGenAiConfigFromStore(requestContext, scope);
    GenAiConfig mergedConfigToUpdate = genAiConfigToUpdate;
    if (configFromStoreOptional.isPresent()) {
      mergedConfigToUpdate =
          genAiConfigHandler.mergeConfigs(genAiConfigToUpdate, configFromStoreOptional.get());
    }
    GenAiConfig updatedConfig =
        genAiConfigStore
            .upsertObject(requestContext, buildScopedGenAiConfig(mergedConfigToUpdate, scope))
            .getData();
    GenAiConfig globalDefaultConfig = getMergedGlobalAndDefaultConfig(requestContext);
    GenAiConfig mergedConfig = genAiConfigHandler.mergeConfigs(updatedConfig, globalDefaultConfig);
    return buildScopedGenAiConfig(mergedConfig, scope);
  }

  private GenAiConfig getMergedGlobalAndDefaultConfig(RequestContext requestContext) {
    GenAiConfig defaultConfig = defaultGenAiConfigProvider.get();
    Optional<GenAiConfig> globalConfigOptional =
        genAiConfigStore.getGenAiConfigFromStore(requestContext, GenAiScope.getDefaultInstance());
    if (globalConfigOptional.isEmpty()) {
      return defaultConfig;
    }
    return genAiConfigHandler.mergeConfigs(globalConfigOptional.get(), defaultConfig);
  }

  private GenAiConfig buildScopedGenAiConfig(GenAiConfig genAiConfig, GenAiScope scope) {
    return genAiConfig.toBuilder().setScope(scope).build();
  }
}
