package ai.traceable.region.config.service;

import com.typesafe.config.Config;

public class RegionConfigServiceConfig {
  private final Config config;

  public RegionConfigServiceConfig(Config config) {
    this.config = config;
  }

  public String getCountriesDataPath() {
    return this.config.getString("neustar.countries.data.path");
  }
}
