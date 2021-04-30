package ai.traceable.blocking.config.service.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import ai.traceable.blocking.config.service.v1.RegionIpBlockingDetails;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.IpRange;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegionBlockingRulesConverterTest {
  private RegionConfigServiceBlockingStub regionConfigServiceStub;
  private MockRegionConfigService mockRegionConfigService;

  private IpRangesConverter ipRangesConverter;

  private RegionBlockingRulesConverter regionBlockingRulesConverter;

  @BeforeEach
  void setup() {
    this.mockRegionConfigService = new MockRegionConfigService();
    this.mockRegionConfigService.start();
    this.regionConfigServiceStub =
        RegionConfigServiceGrpc.newBlockingStub(this.mockRegionConfigService.channel());

    this.ipRangesConverter = mock(IpRangesConverter.class);

    this.regionBlockingRulesConverter =
        new RegionBlockingRulesConverter(this.regionConfigServiceStub, this.ipRangesConverter);
  }

  @Test
  // Region Config Rule A -> blocks Region 1 and Region 2
  // Region Config Rule B -> blocks Region 3
  void convertToBlockingRules() {
    // blocks region 1 and 2
    RegionRule regionRuleA =
        RegionRule.newBuilder()
            .setId("rule-id-1")
            .setName("name-1")
            .addRegionId("region-id-1")
            .addRegionId("region-id-2")
            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
            .build();
    // allows region 3
    RegionRule regionRuleB =
        RegionRule.newBuilder()
            .setId("rule-id-2")
            .setName("name-2")
            .addRegionId("region-id-3")
            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
            .build();

    List<DetailedRegion> regions = this.mockRegionConfigService.getDetailedRegions();

    List<IpRange> region1IpRanges = regions.get(0).getIpRangeList();
    List<IpRange> region2IpRanges = regions.get(1).getIpRangeList();
    List<IpRange> region3IpRanges = regions.get(2).getIpRangeList();

    List<ai.traceable.blocking.config.service.v1.IpRange> convertedIpRanges1 =
        mockIpRangesConverter(region1IpRanges);
    List<ai.traceable.blocking.config.service.v1.IpRange> convertedIpRanges2 =
        mockIpRangesConverter(region2IpRanges);
    List<ai.traceable.blocking.config.service.v1.IpRange> convertedIpRanges3 =
        mockIpRangesConverter(region3IpRanges);

    when(this.ipRangesConverter.convert(region1IpRanges)).thenReturn(convertedIpRanges1);
    when(this.ipRangesConverter.convert(region2IpRanges)).thenReturn(convertedIpRanges2);
    when(this.ipRangesConverter.convert(region3IpRanges)).thenReturn(convertedIpRanges3);

    assertEquals(
        RegionBlockingRules.newBuilder()
            .addAllRegionIpBlockingDetails(
                List.of(
                    RegionIpBlockingDetails.newBuilder()
                        .setRegionId(regions.get(0).getId())
                        .addAllIpRanges(convertedIpRanges1)
                        .build(),
                    RegionIpBlockingDetails.newBuilder()
                        .setRegionId(regions.get(1).getId())
                        .addAllIpRanges(convertedIpRanges2)
                        .build(),
                    RegionIpBlockingDetails.newBuilder()
                        .setRegionId(regions.get(2).getId())
                        .addAllIpRanges(convertedIpRanges3)
                        .build()))
            .build(),
        this.regionBlockingRulesConverter.convert(List.of(regionRuleA, regionRuleB)));
  }

  public List<ai.traceable.blocking.config.service.v1.IpRange> mockIpRangesConverter(
      List<IpRange> regionIpRanges) {
    return regionIpRanges.stream()
        .map(
            regionIpRange ->
                ai.traceable.blocking.config.service.v1.IpRange.newBuilder()
                    .setIpv4Range(
                        ai.traceable.blocking.config.service.v1.IpV4Range.newBuilder()
                            .setStartIp(regionIpRange.getIpv4Range().getStartIp())
                            .setEndIp(regionIpRange.getIpv4Range().getEndIp())
                            .build())
                    .build())
        .collect(Collectors.toList());
  }
}
