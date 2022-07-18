package ai.traceable.blocking.config.service.blockingpolicy.fetchers.config;

import com.typesafe.config.Config;
import java.time.Duration;

public class ActorServiceConfig {

  private static final String ACTOR_SERVICE_CONFIG_NAME = "actor.service.config";
  private static final String HOST_CONFIG_NAME = "host";
  private static final String PORT_CONFIG_NAME = "port";
  private static final String CALL_TIMEOUT_CONFIG_NAME = "request.timeout";

  private final String host;
  private final int port;
  private final Duration callTimeout;

  public ActorServiceConfig(Config config) {
    host = config.getConfig(ACTOR_SERVICE_CONFIG_NAME).getString(HOST_CONFIG_NAME);
    port = config.getConfig(ACTOR_SERVICE_CONFIG_NAME).getInt(PORT_CONFIG_NAME);
    callTimeout = config.getConfig(ACTOR_SERVICE_CONFIG_NAME).getDuration(CALL_TIMEOUT_CONFIG_NAME);
  }

  public String getHost() {
    return this.host;
  }

  public int getPort() {
    return this.port;
  }

  public Duration getCallTimeoutDuration() {
    return this.callTimeout;
  }
}
