package ai.traceable.region.config.service;

import com.typesafe.config.Config;

public class RegionConfigServiceConfig {
  private final Config config;
  private static final String REGION_CONFIG_SERVICE = "region.config.service";
  private static final String SHOULD_PUBLISH_ACTIVITY_EVENTS_CONFIG = "shouldPublishActivityEvents";
  private static final String NEUSTAR_COUNTRIES_DATA_PATH = "neustar.countries.data.path";

  public RegionConfigServiceConfig(Config config) {
    this.config = config.getConfig(REGION_CONFIG_SERVICE);
  }

  public String getCountriesDataPath() {
    return this.config.getString(NEUSTAR_COUNTRIES_DATA_PATH);
  }

  public boolean shouldPublishActivityEvents() {
    return this.config.getBoolean(SHOULD_PUBLISH_ACTIVITY_EVENTS_CONFIG);
  }
}
