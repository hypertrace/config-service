package ai.traceable.iprange.config.service;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import lombok.Getter;

@Getter
public class IpRangeConfigServiceConfig {

  private static final String IPRANGE_CONFIG_SERVICE = "iprange.config.service";
  private static final String MIGRATION_DISABLED_KEY = "migrationDisabled";
  private static final String CHANGE_LOG_1_MIGRATION_DISABLED_KEY =
      "changeLog1." + MIGRATION_DISABLED_KEY;

  private final boolean changeLog1MigrationDisabled;

  @Inject
  IpRangeConfigServiceConfig(Config config) {
    Config config1 = config.getConfig(IPRANGE_CONFIG_SERVICE);
    this.changeLog1MigrationDisabled = config1.getBoolean(CHANGE_LOG_1_MIGRATION_DISABLED_KEY);
  }
}
