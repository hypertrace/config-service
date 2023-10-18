package ai.traceable.blocking.config.service.v1.iptype;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleAggregatorBase.GenericIpTypeRuleConverter;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo.IpRangeInfo;
import ai.traceable.blocking.config.service.v1.IpRange;
import ai.traceable.blocking.config.service.v1.IpType;
import ai.traceable.blocking.config.service.v1.IpTypeRule;
import ai.traceable.blocking.config.service.v1.IpV4Range;
import org.junit.jupiter.api.Test;

class IpTypeRuleConverterTest {

  @Test
  void testRuleConversion() {
    IpTypeRuleInfo commonIpTypeRule =
        IpTypeRuleInfo.builder()
            .ipType(IpTypeRuleInfo.IpType.ANONYMOUS_VPN)
            .ipv4Address(55)
            .ipv4Address(66)
            .ipv4Range(new IpRangeInfo(11, 22))
            .ipv4Range(new IpRangeInfo(33, 44))
            .build();

    IpTypeRule expectedIpTypeRule =
        IpTypeRule.newBuilder()
            .setIpType(IpType.IP_TYPE_VPN)
            .addIpRanges(
                IpRange.newBuilder()
                    .setIpv4Range(IpV4Range.newBuilder().setStartIp(11L).setEndIp(22L)))
            .addIpRanges(
                IpRange.newBuilder()
                    .setIpv4Range(IpV4Range.newBuilder().setStartIp(33L).setEndIp(44L)))
            .addIpRanges(IpRange.newBuilder().setIpv4Address(55))
            .addIpRanges(IpRange.newBuilder().setIpv4Address(66))
            .build();

    GenericIpTypeRuleConverter<IpTypeRule> ipTypeRuleConverter = new IpTypeRuleConverter();
    assertEquals(expectedIpTypeRule, ipTypeRuleConverter.convert(commonIpTypeRule).getLeft());
    assertEquals(
        commonIpTypeRule.getUuid(), ipTypeRuleConverter.convert(commonIpTypeRule).getRight());
  }
}
