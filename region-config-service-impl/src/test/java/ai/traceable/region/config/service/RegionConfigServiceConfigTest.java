package ai.traceable.region.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegionConfigServiceConfigTest {
  private RegionConfigServiceConfig config;
  private static final String MOCK_CONFIG =
      "neustar.countries.data {\n"
          + "    mode = RESOURCE_FILE\n"
          + "    resource.file = neustar/countries.csv\n"
          + "  }\n"
          + "ipqs.countries.data {\n"
          + "    mode = VERSIONS_DIR\n"
          + "    versions.dir = /var/ipqs/country_csv\n"
          + "    versions.file.name = countries.csv\n"
          + "    versions.refresh.duration = 24h\n"
          + "}";

  @BeforeEach
  void setup() {
    this.config = new RegionConfigServiceConfig(mockConfig());
  }

  @Test
  void shouldParseConfig() {
    RegionConfigServiceConfig.CountriesDataConfig neustarConfig =
        config.getNeustarCountriesDataConfig();
    assertEquals("neustar/countries.csv", neustarConfig.getResourceFile());
    assertNull(neustarConfig.getVersionsDir());
    assertNull(neustarConfig.getVersionsFileName());
    assertNull(neustarConfig.getVersionRefreshDuration());
    RegionConfigServiceConfig.CountriesDataConfig ipqsConfig = config.getIpqsCountriesDataConfig();
    assertEquals("/var/ipqs/country_csv", ipqsConfig.getVersionsDir());
    assertEquals("countries.csv", ipqsConfig.getVersionsFileName());
    assertEquals(Duration.ofHours(24), ipqsConfig.getVersionRefreshDuration());
    assertNull(ipqsConfig.getResourceFile());
  }

  static Config mockConfig() {
    Config mockConfig = mock(Config.class);
    when(mockConfig.getConfig("region.config.service"))
        .thenReturn(ConfigFactory.parseString(MOCK_CONFIG));
    return mockConfig;
  }
}
