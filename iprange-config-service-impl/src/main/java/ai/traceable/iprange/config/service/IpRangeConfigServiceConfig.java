package ai.traceable.iprange.config.service;

import com.typesafe.config.Config;
import lombok.Getter;

public class IpRangeConfigServiceConfig {
  private final Config config;
  private static final String IPRANGE_CONFIG_SERVICE = "iprange.config.service";
  private static final String SHOULD_PUBLISH_ACTIVITY_EVENTS_CONFIG = "shouldPublishActivityEvents";
  private static final String MIGRATION_DISABLED_KEY = "migrationDisabled";
  private static final String CHANGE_LOG_1_MIGRATION_DISABLED_KEY =
      "changeLog1." + MIGRATION_DISABLED_KEY;
  @Getter private final boolean changeLog1MigrationDisabled;

  IpRangeConfigServiceConfig(Config config) {
    this.config = config.getConfig(IPRANGE_CONFIG_SERVICE);
    this.changeLog1MigrationDisabled = this.config.getBoolean(CHANGE_LOG_1_MIGRATION_DISABLED_KEY);
  }

  public boolean shouldPublishActivityEvents() {
    return this.config.getBoolean(SHOULD_PUBLISH_ACTIVITY_EVENTS_CONFIG);
  }
}
