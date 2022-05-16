package ai.traceable.localprocessing.config.service.config;

import com.google.inject.Inject;
import com.typesafe.config.Config;

public class EntityQueryServiceConfig {
  private final Config config;
  private final Config attributesMapConfig;

  @Inject
  public EntityQueryServiceConfig(Config config) {
    this.config = config.getConfig("entity.service.config");
    this.attributesMapConfig = config.getConfig("entity.service.attributeMap");
  }

  public String getEntityServiceHost() {
    return this.config.getString("host");
  }

  public Integer getEntityServicePort() {
    return this.config.getInt("port");
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
}
