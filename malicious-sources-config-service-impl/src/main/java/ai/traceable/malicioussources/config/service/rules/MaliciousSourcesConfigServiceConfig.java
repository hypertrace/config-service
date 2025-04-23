package ai.traceable.malicioussources.config.service.rules;

import com.typesafe.config.Config;
import lombok.Getter;

public class MaliciousSourcesConfigServiceConfig {
  private static final String MALICIOUS_SOURCES_CONFIG_KEY = "malicious.sources.config.service.";
  private static final String MIGRATION_DISABLED_KEY = "migrationDisabled";
  private static final String CHANGE_LOG_1_MIGRATION_DISABLED_KEY =
      MALICIOUS_SOURCES_CONFIG_KEY + "changeLog1." + MIGRATION_DISABLED_KEY;
  @Getter private final boolean changeLog1MigrationDisabled;

  public MaliciousSourcesConfigServiceConfig(Config config) {
    this.changeLog1MigrationDisabled = config.getBoolean(CHANGE_LOG_1_MIGRATION_DISABLED_KEY);
  }
}
