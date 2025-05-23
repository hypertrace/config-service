package ai.traceable.region.config.service;

import ai.traceable.config.utils.refresh.FileRefreshConfig;
import com.typesafe.config.Config;
import lombok.Getter;

public class RegionConfigServiceConfig {
  private final Config config;
  private static final String REGION_CONFIG_SERVICE = "region.config.service";
  private static final String NEUSTAR_COUNTRIES_DATA_CONFIG = "neustar.countries.data";
  private static final String IPQS_COUNTRIES_DATA_CONFIG = "ipqs.countries.data";
  private static final String IPQS_NEUSTAR_RESOLUTION_ENABLED_CONFIG =
      "ipqs.countries.data.resolve.neustar";
  private static final String MIGRATION_DISABLED_KEY = "migrationDisabled";
  private static final String CHANGE_LOG_1_MIGRATION_DISABLED_KEY =
      "changeLog1." + MIGRATION_DISABLED_KEY;
  @Getter private final boolean changeLog1MigrationDisabled;

  public RegionConfigServiceConfig(Config config) {
    this.config = config.getConfig(REGION_CONFIG_SERVICE);
    this.changeLog1MigrationDisabled = this.config.getBoolean(CHANGE_LOG_1_MIGRATION_DISABLED_KEY);
  }

  public FileRefreshConfig getNeustarCountriesDataConfig() {
    return new FileRefreshConfig(config.getConfig(NEUSTAR_COUNTRIES_DATA_CONFIG));
  }

  public FileRefreshConfig getIpqsCountriesDataConfig() {
    return new FileRefreshConfig(config.getConfig(IPQS_COUNTRIES_DATA_CONFIG));
  }

  public boolean getIpqsNeustarResolutionEnabled() {
    return config.hasPath(IPQS_NEUSTAR_RESOLUTION_ENABLED_CONFIG)
        && config.getBoolean(IPQS_NEUSTAR_RESOLUTION_ENABLED_CONFIG);
  }
}
