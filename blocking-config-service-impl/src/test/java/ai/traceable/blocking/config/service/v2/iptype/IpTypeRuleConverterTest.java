package ai.traceable.blocking.config.service.v2.iptype;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleAggregatorBase.GenericIpTypeRuleConverter;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo.IpRangeInfo;
import ai.traceable.blocking.config.service.v2.IpRange;
import ai.traceable.blocking.config.service.v2.IpType;
import ai.traceable.blocking.config.service.v2.IpTypeRule;
import ai.traceable.blocking.config.service.v2.IpV4Range;
import org.junit.jupiter.api.Test;

class IpTypeRuleConverterTest {

  @Test
  void testRuleConversion() {
    IpTypeRuleInfo ipTypeRuleInfo = new IpTypeRuleInfo(IpTypeRuleInfo.IpType.ANONYMOUS_VPN);
    ipTypeRuleInfo.getIpv4Ranges().add(new IpRangeInfo(11, 22));
    ipTypeRuleInfo.getIpv4Ranges().add(new IpRangeInfo(33, 44));
    ipTypeRuleInfo.getIpv4Addresses().add(55);
    ipTypeRuleInfo.getIpv4Addresses().add(66);

    IpTypeRule expectedIpTypeRule =
        IpTypeRule.newBuilder()
            .setIpType(IpType.IP_TYPE_VPN)
            .addIpRanges(
                IpRange.newBuilder()
                    .setIpv4Range(IpV4Range.newBuilder().setStartIp(11).setEndIp(22)))
            .addIpRanges(
                IpRange.newBuilder()
                    .setIpv4Range(IpV4Range.newBuilder().setStartIp(33).setEndIp(44)))
            .addIpRanges(IpRange.newBuilder().setIpv4Address(55))
            .addIpRanges(IpRange.newBuilder().setIpv4Address(66))
            .build();

    GenericIpTypeRuleConverter<IpTypeRule> ipTypeRuleConverter = new IpTypeRuleConverter();
    assertEquals(expectedIpTypeRule, ipTypeRuleConverter.convert(ipTypeRuleInfo));
  }
}
