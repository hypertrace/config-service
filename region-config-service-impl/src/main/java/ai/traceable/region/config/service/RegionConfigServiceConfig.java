package ai.traceable.region.config.service;

import com.typesafe.config.Config;
import java.time.Duration;
import lombok.Value;

public class RegionConfigServiceConfig {
  private final Config config;
  private static final String REGION_CONFIG_SERVICE = "region.config.service";
  private static final String SHOULD_PUBLISH_ACTIVITY_EVENTS_CONFIG = "shouldPublishActivityEvents";
  private static final String NEUSTAR_COUNTRIES_DATA_CONFIG = "neustar.countries.data";
  private static final String IPQS_COUNTRIES_DATA_CONFIG = "ipqs.countries.data";

  public RegionConfigServiceConfig(Config config) {
    this.config = config.getConfig(REGION_CONFIG_SERVICE);
  }

  public CountriesDataConfig getNeustarCountriesDataConfig() {
    return new CountriesDataConfig(config.getConfig(NEUSTAR_COUNTRIES_DATA_CONFIG));
  }

  public CountriesDataConfig getIpqsCountriesDataConfig() {
    return new CountriesDataConfig(config.getConfig(IPQS_COUNTRIES_DATA_CONFIG));
  }

  public boolean shouldPublishActivityEvents() {
    return this.config.getBoolean(SHOULD_PUBLISH_ACTIVITY_EVENTS_CONFIG);
  }

  @Value
  public static class CountriesDataConfig {
    public enum Mode {
      RESOURCE_FILE,
      VERSIONS_DIR
    }

    private static final String MODE_CONFIG_KEY = "mode";
    private static final String VERSIONS_DIR_CONFIG_KEY = "versions.dir";
    private static final String VERSIONS_FILE_NAME_CONFIG_KEY = "versions.file.name";
    private static final String VERSIONS_REFRESH_DURATION_CONFIG_KEY = "versions.refresh.duration";
    private static final String RESOURCE_FILE_CONFIG_KEY = "resource.file";
    Mode mode;
    String versionsDir;
    String versionsFileName;
    Duration versionRefreshDuration;
    String resourceFile; // path of file checked in repo under resources

    public CountriesDataConfig(Config dataConfig) {
      switch (dataConfig.getEnum(Mode.class, MODE_CONFIG_KEY)) {
        case RESOURCE_FILE:
          this.mode = Mode.RESOURCE_FILE;
          this.resourceFile = dataConfig.getString(RESOURCE_FILE_CONFIG_KEY);
          this.versionsDir = null;
          this.versionsFileName = null;
          this.versionRefreshDuration = null;
          break;
        case VERSIONS_DIR:
          this.mode = Mode.VERSIONS_DIR;
          this.versionsDir = dataConfig.getString(VERSIONS_DIR_CONFIG_KEY);
          this.versionsFileName = dataConfig.getString(VERSIONS_FILE_NAME_CONFIG_KEY);
          this.versionRefreshDuration =
              dataConfig.getDuration(VERSIONS_REFRESH_DURATION_CONFIG_KEY);
          this.resourceFile = null;
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported data config mode for CountriesDataConfig");
      }
    }
  }
}
