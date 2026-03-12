package ai.traceable.entity.fetcher.cache.config;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import java.time.Duration;

public class EntityQueryServiceConfig {
  private static final String TIMEOUT_CONFIG_KEY = "timeout";

  private final Config serviceConfig;
  private final Config attributesMapConfig;

  @Inject
  public EntityQueryServiceConfig(Config config) {
    config = config.getConfig("entity.service");
    this.serviceConfig = config.getConfig("config");
    this.attributesMapConfig = config.getConfig("attributeMap");
  }

  public String getEntityServiceHost() {
    return this.serviceConfig.getString("host");
  }

  public Integer getEntityServicePort() {
    return this.serviceConfig.getInt("port");
  }

  public Duration getTimeout() {
    return this.serviceConfig.getDuration(TIMEOUT_CONFIG_KEY);
  }

  public String getServiceIdColumnName() {
    return this.attributesMapConfig.getString("service.id");
  }

  public String getServiceNameColumnName() {
    return this.attributesMapConfig.getString("service.name");
  }

  public String getServiceEnvironmentColumnName() {
    return this.attributesMapConfig.getString("service.environment");
  }

  public String getApiIdColumnName() {
    return this.attributesMapConfig.getString("api.id");
  }

  public String getApiNameColumnName() {
    return this.attributesMapConfig.getString("api.name");
  }

  public String getApiUrlPatternColumnName() {
    return this.attributesMapConfig.getString("api.url.pattern");
  }

  public String getApiResolvedUrlPatternsColumnName() {
    return this.attributesMapConfig.getString("api.resolved.url.patterns");
  }

  public String getApiLabelsColumnName() {
    return this.attributesMapConfig.getString("api.labels");
  }

  public String getApiIsLearntStatusColumnName() {
    return this.attributesMapConfig.getString("api.isLearnt");
  }

  public String getApiTypeColumnName() {
    return this.attributesMapConfig.getString("api.type");
  }

  public String getHttpMethodColumnName() {
    return this.attributesMapConfig.getString("api.httpMethod");
  }

  public String getApiEnvironmentColumnName() {
    return this.attributesMapConfig.getString("api.environment");
  }

  public String getApiServiceNameColumnName() {
    return this.attributesMapConfig.getString("api.serviceName");
  }

  public String getIsGenAiEndpointColumnName() {
    return this.attributesMapConfig.getString("api.isGenAi");
  }

  public String getApiAssociatedAiModelsColumnName() {
    return this.attributesMapConfig.getString("api.associatedAiModels");
  }

  public String getApiAssociatedAiVendorsColumnName() {
    return this.attributesMapConfig.getString("api.associatedAiVendors");
  }

  public String getApiPromptAttributeKeysColumnName() {
    return this.attributesMapConfig.getString("api.promptAttributeKeys");
  }
}
