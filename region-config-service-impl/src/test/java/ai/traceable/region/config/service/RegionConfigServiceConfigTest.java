package ai.traceable.region.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegionConfigServiceConfigTest {
  private RegionConfigServiceConfig config;

  @BeforeEach
  void setup() {
    this.config = new RegionConfigServiceConfig(mockConfig());
  }

  @Test
  void shouldParseConfig() {
    assertEquals("/neustar/countries.csv", config.getCountriesDataPath());
  }

  private Config mockConfig() {
    Config mockConfig = mock(Config.class);
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("neustar.countries.data.path", "/neustar/countries.csv");
    when(mockConfig.getConfig("region.config.service"))
        .thenReturn(ConfigFactory.parseMap(configMap));
    return mockConfig;
  }
}
