package ai.traceable.blocking.config.service.v2.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.v2.RegionIpBlockingRule;
import ai.traceable.region.config.service.v1.Country;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.IpRange;
import ai.traceable.region.config.service.v1.IpV4Range;
import ai.traceable.region.config.service.v1.Region;
import java.util.List;
import org.junit.jupiter.api.Test;

class RegionIpRulesConverterTest {

  @Test
  void convertToBlockingRules() {
    ai.traceable.blocking.config.service.v2.regions.IpRangesConverter mockIpRangesConverter =
        mock(ai.traceable.blocking.config.service.v2.regions.IpRangesConverter.class);
    ai.traceable.blocking.config.service.v2.regions.RegionIpRulesConverter regionIpRulesConverter =
        new ai.traceable.blocking.config.service.v2.regions.RegionIpRulesConverter(
            mockIpRangesConverter);

    final DetailedRegion detailedRegion =
        DetailedRegion.newBuilder()
            .setId("region-id-1")
            .setName("region-1")
            .addIpRange(
                IpRange.newBuilder()
                    .setIpv4Range(IpV4Range.newBuilder().setStartIp(123L).setEndIp(789L))
                    .build())
            .setRegion(Region.newBuilder().setCountry(Country.newBuilder().setIsoCode("BD")))
            .build();

    doReturn(List.of(ai.traceable.blocking.config.service.v2.IpRange.getDefaultInstance()))
        .when(mockIpRangesConverter)
        .convert(
            List.of(
                IpRange.newBuilder()
                    .setIpv4Range(IpV4Range.newBuilder().setStartIp(123L).setEndIp(789L))
                    .build()));

    final RegionIpBlockingRule expectedIpBlockingRule =
        RegionIpBlockingRule.newBuilder()
            .setRegionId("BD")
            .addIpRanges(ai.traceable.blocking.config.service.v2.IpRange.getDefaultInstance())
            .build();

    assertEquals(expectedIpBlockingRule, regionIpRulesConverter.convert(detailedRegion));
  }
}
