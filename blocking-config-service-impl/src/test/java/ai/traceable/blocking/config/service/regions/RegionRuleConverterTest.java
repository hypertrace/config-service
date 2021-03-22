package ai.traceable.blocking.config.service.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.v1.BlockingInfo;
import ai.traceable.blocking.config.service.v1.BlockingRule;
import ai.traceable.blocking.config.service.v1.IpBlocking;
import ai.traceable.blocking.config.service.v1.RuleActionType;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.IpRange;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegionRuleConverterTest {
  private RegionConfigServiceBlockingStub regionConfigServiceStub;
  private MockRegionConfigService mockRegionConfigService;

  private ActionTypeConverter actionTypeConverter;
  private IpBlockingConverter ipBlockingConverter;
  private BlockingInfoConverter blockingInfoConverter;

  private RegionRuleConverter regionRuleConverter;

  @BeforeEach
  void setup() {
    this.mockRegionConfigService = new MockRegionConfigService();
    this.mockRegionConfigService.start();
    this.regionConfigServiceStub =
        RegionConfigServiceGrpc.newBlockingStub(this.mockRegionConfigService.channel());

    this.actionTypeConverter = mock(ActionTypeConverter.class);
    this.ipBlockingConverter = mock(IpBlockingConverter.class);
    this.blockingInfoConverter = mock(BlockingInfoConverter.class);

    this.regionRuleConverter =
        new RegionRuleConverter(
            this.regionConfigServiceStub,
            this.actionTypeConverter,
            this.ipBlockingConverter,
            this.blockingInfoConverter);
  }

  @Test
  // Region Config Rule A -> blocks Region 1 and Region 2
  // Region Config Rule B -> allows Region 3
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
            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALLOW)
            .build();

    when(this.actionTypeConverter.convert(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK))
        .thenReturn(Optional.of(RuleActionType.RULE_ACTION_TYPE_BLOCK));
    when(this.actionTypeConverter.convert(RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALLOW))
        .thenReturn(Optional.of(RuleActionType.RULE_ACTION_TYPE_ALLOW));

    List<DetailedRegion> regions = this.mockRegionConfigService.getDetailedRegions();

    List<IpRange> region1IpRanges = regions.get(0).getIpRangeList();
    List<IpRange> region2IpRanges = regions.get(1).getIpRangeList();
    List<IpRange> region3IpRanges = regions.get(2).getIpRangeList();

    IpBlocking ipBlocking1 =
        IpBlocking.newBuilder()
            .addIpRange(
                ai.traceable.blocking.config.service.v1.IpRange.newBuilder()
                    .setIpv4Range(
                        ai.traceable.blocking.config.service.v1.IpV4Range.newBuilder()
                            .setStartIp(123L)
                            .setEndIp(789L)
                            .build())
                    .setIpv4Range(
                        ai.traceable.blocking.config.service.v1.IpV4Range.newBuilder()
                            .setStartIp(123L)
                            .setEndIp(234L)
                            .build())
                    .build())
            .addIpRange(
                ai.traceable.blocking.config.service.v1.IpRange.newBuilder()
                    .setIpv4Range(
                        ai.traceable.blocking.config.service.v1.IpV4Range.newBuilder()
                            .setStartIp(234L)
                            .setEndIp(456L)
                            .build())
                    .build())
            .build();

    IpBlocking ipBlocking2 =
        IpBlocking.newBuilder()
            .addIpRange(
                ai.traceable.blocking.config.service.v1.IpRange.newBuilder()
                    .setIpv4Range(
                        ai.traceable.blocking.config.service.v1.IpV4Range.newBuilder()
                            .setStartIp(456L)
                            .setEndIp(678L)
                            .build())
                    .build())
            .build();

    when(this.ipBlockingConverter.convert(
            Lists.newArrayList(Iterables.concat(region1IpRanges, region2IpRanges))))
        .thenReturn(ipBlocking1);
    when(this.ipBlockingConverter.convert(region3IpRanges)).thenReturn(ipBlocking2);

    BlockingInfo blockingInfo1 = BlockingInfo.newBuilder().setInfo("info-1").build();
    BlockingInfo blockingInfo2 = BlockingInfo.newBuilder().setInfo("info-2").build();

    when(this.blockingInfoConverter.convert(regionRuleA)).thenReturn(Optional.of(blockingInfo1));
    when(this.blockingInfoConverter.convert(regionRuleB)).thenReturn(Optional.of(blockingInfo2));

    assertEquals(
        List.of(
            BlockingRule.newBuilder()
                .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK)
                .setIpBlocking(ipBlocking1)
                .setBlockingInfo(blockingInfo1)
                .build(),
            BlockingRule.newBuilder()
                .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                .setIpBlocking(ipBlocking2)
                .setBlockingInfo(blockingInfo2)
                .build()),
        this.regionRuleConverter.convert(List.of(regionRuleA, regionRuleB)));
  }
}
