package ai.traceable.blocking.config.service.entity;

import ai.traceable.blocking.config.service.BlockingDataCacheConfig;
import com.google.inject.Inject;
import com.typesafe.config.Config;

public class EntityQueryServiceConfig {
  private final Config config;
  private final Config attributesMapConfig;

  @Inject
  public EntityQueryServiceConfig(Config config) {
    this.config = config.getConfig("entity.service.config");
    this.attributesMapConfig = config.getConfig("attributeMap");
  }

  public String getEntityServiceHost() {
    return this.config.getString("host");
  }

  public Integer getEntityServicePort() {
    return this.config.getInt("port");
  }

  public String getEnvironmentIdColumnName() {
    return this.attributesMapConfig.getString("environment.id");
  }

  public String getEnvironmentNameColumnName() {
    return this.attributesMapConfig.getString("environment.name");
  }

  public BlockingDataCacheConfig getCacheConfig() {
    return new BlockingDataCacheConfig(config);
  }
}
