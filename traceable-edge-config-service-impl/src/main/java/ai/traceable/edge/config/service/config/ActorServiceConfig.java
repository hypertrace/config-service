package ai.traceable.edge.config.service.config;

import com.typesafe.config.Config;
import java.time.Duration;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@ToString
public class ActorServiceConfig {

  private static final String ACTOR_SERVICE_CONFIG_NAME = "actor.service.config";
  private static final String HOST_CONFIG_NAME = "host";
  private static final String PORT_CONFIG_NAME = "port";
  private static final String CALL_TIMEOUT_CONFIG_NAME = "request.timeout";
  private static final String MAX_NUMBER_OF_ACTORS = "maxNumberOfActors";

  String host;
  int port;
  Duration callTimeoutDuration;
  CacheConfig cacheConfig;
  int maxNumberOfActors;

  public ActorServiceConfig(Config config) {
    Config actorConfig = config.getConfig(ACTOR_SERVICE_CONFIG_NAME);
    host = actorConfig.getString(HOST_CONFIG_NAME);
    port = actorConfig.getInt(PORT_CONFIG_NAME);
    callTimeoutDuration = actorConfig.getDuration(CALL_TIMEOUT_CONFIG_NAME);
    cacheConfig = new CacheConfig(actorConfig);
    maxNumberOfActors = actorConfig.getInt(MAX_NUMBER_OF_ACTORS);
  }
}
