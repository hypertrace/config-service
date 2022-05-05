package ai.traceable.ratelimiting.service.v2;

import com.typesafe.config.Config;

public class RateLimitingConfigServiceConfig {
  private final Config config;
  private static final String RATE_LIMITING_CONFIG_SERVICE = "rate.limiting.config.service";
  private static final String SHOULD_PUBLISH_ACTIVITY_EVENTS = "shouldPublishActivityEvents";

  public RateLimitingConfigServiceConfig(Config config) {
    this.config = config.getConfig(RATE_LIMITING_CONFIG_SERVICE);
  }

  public boolean shouldPublishActivityEvents() {
    return this.config.hasPath(SHOULD_PUBLISH_ACTIVITY_EVENTS)
        && this.config.getBoolean(SHOULD_PUBLISH_ACTIVITY_EVENTS);
  }
}
