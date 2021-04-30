package ai.traceable.region.config.service.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.IpRange;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegionConverterTest {
  private IpRangeConverter ipRangeConverter;
  private RegionConverter regionConverter;

  @BeforeEach
  void setup() {
    this.ipRangeConverter = mock(IpRangeConverter.class);
    this.regionConverter = new RegionConverter(this.ipRangeConverter);
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

  @Test
  void shouldConvertToDetailedRegion() {
    List<IpV4Range> ipV4Ranges = List.of(new IpV4Range(123L, 789L));
    Region region = new Region("region-id", "region-name", RegionType.COUNTRY, ipV4Ranges);
    List<IpRange> ipRanges = List.of(IpRange.getDefaultInstance());
    when(this.ipRangeConverter.convert(ipV4Ranges)).thenReturn(ipRanges);

    assertEquals(
        DetailedRegion.newBuilder()
            .setId("region-id")
            .setName("region-name")
            .addAllIpRange(ipRanges)
            .build(),
        regionConverter.convertToDetailedRegion(region));
  }
}
