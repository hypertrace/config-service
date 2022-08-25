package ai.traceable.localprocessing.config.service.apinaming.http;

import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.localprocessing.config.service.apinaming.http.namingconfig.HttpApiNamingCachedConfigManager;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.LocalApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.config.http.HttpApiNamingConfig;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class LocalApiNamingConfigManager {

  private static final String LOCAL_API_NAMING_DEFAULT_CONFIG_KEY = "default";
  private static final String TRIE_VERSION_CONFIG_KEY = "version";
  private static final String LOCAL_API_NAMING_DISABLED_CONFIG_KEY = "disabled";
  private final HttpApiNamingConfig httpApiNamingConfig;
  private final HttpApiNamingCachedConfigManager httpApiNamingCachedConfigManager;

  @Inject
  public LocalApiNamingConfigManager(
      HttpApiNamingConfig httpApiNamingConfig,
      HttpApiNamingCachedConfigManager httpApiNamingCachedConfigManager) {
    this.httpApiNamingConfig = httpApiNamingConfig;
    this.httpApiNamingCachedConfigManager = httpApiNamingCachedConfigManager;
  }

  public LocalApiNamingConfigInfo getLocalApiNamingConfigInfo(
      RequestContext requestContext, String serviceId) throws ExecutionException {
    String tenantId = requestContext.getTenantId().orElseThrow();

    List<TrainingConfig> trainingConfigs =
        httpApiNamingCachedConfigManager
            .getTrainingConfigListForService(requestContext, serviceId)
            .orElse(Collections.emptyList());
    Config fullTrieReloadConfig = httpApiNamingConfig.getFullTrieReloadConfig();
    LocalApiNamingConfigInfo localApiNamingConfigInfo;
    if (fullTrieReloadConfig.hasPath(tenantId)) {
      Config tenantSpecificConfig = fullTrieReloadConfig.getConfig(tenantId);
      if (tenantSpecificConfig.hasPath(serviceId)) {
        localApiNamingConfigInfo =
            getLocalApiNamingConfigInfo(trainingConfigs, tenantSpecificConfig.getConfig(serviceId));
      } else {
        localApiNamingConfigInfo =
            getLocalApiNamingConfigInfo(
                trainingConfigs,
                tenantSpecificConfig.getConfig(LOCAL_API_NAMING_DEFAULT_CONFIG_KEY));
      }
    } else {
      localApiNamingConfigInfo =
          getLocalApiNamingConfigInfo(
              trainingConfigs, fullTrieReloadConfig.getConfig(LOCAL_API_NAMING_DEFAULT_CONFIG_KEY));
    }
    return localApiNamingConfigInfo;
  }

  private LocalApiNamingConfigInfo getLocalApiNamingConfigInfo(
      List<TrainingConfig> trainingConfigs, Config config) {
    Optional<TrainingConfig> localTrainingConfig =
        trainingConfigs.stream()
            .filter(TrainingConfig::hasLocalTrainingConfig)
            .filter(trainingConfig -> trainingConfig.getLocalTrainingConfig().hasApiNamingConfig())
            .findAny();
    // by default local api naming config is disabled
    boolean disabled = localTrainingConfig.map(TrainingConfig::getDisabled).orElse(true);
    return new LocalApiNamingConfigInfo(disabled, config.getString(TRIE_VERSION_CONFIG_KEY));
  }
}
