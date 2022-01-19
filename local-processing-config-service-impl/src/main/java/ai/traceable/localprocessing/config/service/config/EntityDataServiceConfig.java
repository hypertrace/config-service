package ai.traceable.localprocessing.config.service.config;

import com.google.inject.Inject;
import com.typesafe.config.Config;

public class EntityDataServiceConfig {
  private final Config config;

  @Inject
  public EntityDataServiceConfig(Config config) {
    this.config = config.getConfig("entity.service.config");
  }

  public String getEntityServiceHost() {
    return this.config.getString("host");
  }

  public Integer getEntityServicePort() {
    return this.config.getInt("port");
  }
}
