package ai.traceable.iprange.config.service;

import com.typesafe.config.Config;

public class IpRangeConfigServiceConfig {
  private final Config config;
  private static final String IPRANGE_CONFIG_SERVICE = "iprange.config.service";
  private static final String SHOULD_PUBLISH_ACTIVITY_EVENTS_CONFIG = "shouldPublishActivityEvents";

  IpRangeConfigServiceConfig(Config config) {
    this.config = config.getConfig(IPRANGE_CONFIG_SERVICE);
  }

  public boolean shouldPublishActivityEvents() {
    return this.config.getBoolean(SHOULD_PUBLISH_ACTIVITY_EVENTS_CONFIG);
  }
}
