package ai.traceable.localprocessing.config.service.apinaming.http;

import ai.traceable.localprocessing.config.service.apinaming.http.utils.LocalApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.config.http.HttpApiNamingConfig;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class LocalApiNamingConfigManager {

  private static final String LOCAL_API_NAMING_DEFAULT_CONFIG_KEY = "default";
  private static final String TRIE_VERSION_CONFIG_KEY = "version";
  private static final String LOCAL_API_NAMING_DISABLED_CONFIG_KEY = "disabled";
  private final HttpApiNamingConfig httpApiNamingConfig;

  @Inject
  public LocalApiNamingConfigManager(HttpApiNamingConfig httpApiNamingConfig) {
    this.httpApiNamingConfig = httpApiNamingConfig;
  }

  public LocalApiNamingConfigInfo getLocalApiNamingConfigInfo(
      RequestContext requestContext, String serviceId) {
    String tenantId = requestContext.getTenantId().orElseThrow();
    Config fullTrieReloadConfig = httpApiNamingConfig.getFullTrieReloadConfig();
    LocalApiNamingConfigInfo localApiNamingConfigInfo;
    if (fullTrieReloadConfig.hasPath(tenantId)) {
      Config tenantSpecificConfig = fullTrieReloadConfig.getConfig(tenantId);
      if (tenantSpecificConfig.hasPath(serviceId)) {
        localApiNamingConfigInfo =
            getLocalApiNamingConfigInfo(tenantSpecificConfig.getConfig(serviceId));
      } else {
        localApiNamingConfigInfo =
            getLocalApiNamingConfigInfo(
                tenantSpecificConfig.getConfig(LOCAL_API_NAMING_DEFAULT_CONFIG_KEY));
      }
    } else {
      localApiNamingConfigInfo =
          getLocalApiNamingConfigInfo(
              fullTrieReloadConfig.getConfig(LOCAL_API_NAMING_DEFAULT_CONFIG_KEY));
    }
    return localApiNamingConfigInfo;
  }

  private LocalApiNamingConfigInfo getLocalApiNamingConfigInfo(Config config) {
    return new LocalApiNamingConfigInfo(
        config.getBoolean(LOCAL_API_NAMING_DISABLED_CONFIG_KEY),
        config.getString(TRIE_VERSION_CONFIG_KEY));
  }
}
