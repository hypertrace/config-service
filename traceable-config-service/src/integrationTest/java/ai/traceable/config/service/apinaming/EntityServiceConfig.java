package ai.traceable.config.service.apinaming;

import com.typesafe.config.Config;
import javax.inject.Inject;

public class EntityServiceConfig {
  private final Config config;

  @Inject
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
