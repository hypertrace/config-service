package ai.traceable.config.service.apinaming;

import com.typesafe.config.Config;

public class EntityServiceConfig {
  private final Config config;

  public EntityServiceConfig(Config config) {
    this.config = config.getConfig("entity.service.config");
  }

  public String getEntityServiceHost() {
    return this.config.getString("host");
  }

  public Integer getEntityServicePort() {
    return this.config.getInt("port");
  }
}
