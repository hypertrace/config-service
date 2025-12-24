package ai.traceable.entity.fetcher.cache.config;

import jakarta.inject.Inject;
import java.time.Duration;
import lombok.Getter;

@Getter
public class EntityRelationServiceConfig {

  private static final String TIMEOUT_CONFIG_KEY = "timeout";
  private static final String HOST_CONFIG_KEY = "host";
  private static final String PORT_CONFIG_KEY = "port";
  public static final String ENTITY_RELATION_SERVICE = "entity.relation.service";

  private final String host;
  private final int port;
  private final Duration timeout;

  @Inject
  public EntityRelationServiceConfig(com.typesafe.config.Config config) {
    config = config.getConfig(ENTITY_RELATION_SERVICE);
    this.host = config.getString(HOST_CONFIG_KEY);
    this.port = config.getInt(PORT_CONFIG_KEY);
    this.timeout = config.getDuration(TIMEOUT_CONFIG_KEY);
  }
}
