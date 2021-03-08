package ai.traceable.region.config.service.regions;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegionConverterTest {
  private RegionConverter regionConverter;

  @BeforeEach
  void setup() {
    this.regionConverter = new RegionConverter();
  }

  @Test
  void shouldConvertRegion() {
    Region region =
        new Region("region-id", "region-name", RegionType.COUNTRY, Collections.emptyList());
    assertEquals(
        ai.traceable.region.config.service.v1.Region.newBuilder()
            .setId("region-id")
            .setName("region-name")
            .build(),
        regionConverter.convert(region));
  }
}
