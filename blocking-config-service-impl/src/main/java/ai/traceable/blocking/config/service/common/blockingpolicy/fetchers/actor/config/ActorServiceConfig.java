package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.config;

import ai.traceable.blocking.config.service.common.BlockingDataCacheConfig;
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

  private static final String ACTOR_FETCHER_CONFIG_NAME = "actor.fetcher.config";
  private static final String HOST_CONFIG_NAME = "host";
  private static final String PORT_CONFIG_NAME = "port";
  private static final String CALL_TIMEOUT_CONFIG_NAME = "request.timeout";
  private static final String MAX_NUMBER_OF_ACTORS = "maxNumberOfActors";

  String host;
  int port;
  Duration callTimeoutDuration;
  BlockingDataCacheConfig cacheConfig;
  int maxNumberOfActors;

  public ActorServiceConfig(Config config) {
    host = config.getConfig(ACTOR_FETCHER_CONFIG_NAME).getString(HOST_CONFIG_NAME);
    port = config.getConfig(ACTOR_FETCHER_CONFIG_NAME).getInt(PORT_CONFIG_NAME);
    callTimeoutDuration =
        config.getConfig(ACTOR_FETCHER_CONFIG_NAME).getDuration(CALL_TIMEOUT_CONFIG_NAME);
    cacheConfig = new BlockingDataCacheConfig(config.getConfig(ACTOR_FETCHER_CONFIG_NAME));
    maxNumberOfActors = config.getConfig(ACTOR_FETCHER_CONFIG_NAME).getInt(MAX_NUMBER_OF_ACTORS);
  }
}
