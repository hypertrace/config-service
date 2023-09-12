package ai.traceable.region.config.service;

import ai.traceable.config.utils.refresh.FileRefreshConfig;
import com.typesafe.config.Config;

public class RegionConfigServiceConfig {
  private final Config config;
  private static final String REGION_CONFIG_SERVICE = "region.config.service";
  private static final String SHOULD_PUBLISH_ACTIVITY_EVENTS_CONFIG = "shouldPublishActivityEvents";
  private static final String NEUSTAR_COUNTRIES_DATA_CONFIG = "neustar.countries.data";
  private static final String IPQS_COUNTRIES_DATA_CONFIG = "ipqs.countries.data";

  public RegionConfigServiceConfig(Config config) {
    this.config = config.getConfig(REGION_CONFIG_SERVICE);
  }

  public FileRefreshConfig getNeustarCountriesDataConfig() {
    return new FileRefreshConfig(config.getConfig(NEUSTAR_COUNTRIES_DATA_CONFIG));
  }

  public FileRefreshConfig getIpqsCountriesDataConfig() {
    return new FileRefreshConfig(config.getConfig(IPQS_COUNTRIES_DATA_CONFIG));
  }

  public boolean shouldPublishActivityEvents() {
    return this.config.getBoolean(SHOULD_PUBLISH_ACTIVITY_EVENTS_CONFIG);
  }
}
