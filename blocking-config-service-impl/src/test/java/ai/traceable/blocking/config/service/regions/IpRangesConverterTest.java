package ai.traceable.blocking.config.service.regions;

import static org.junit.Assert.assertEquals;

import ai.traceable.region.config.service.v1.IpRange;
import ai.traceable.region.config.service.v1.IpV4Range;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IpRangesConverterTest {
  private IpRangesConverter ipRangesConverter;

  @BeforeEach
  void setup() {
    this.ipRangesConverter = new IpRangesConverter();
  }

  @Test
  void convertToIpBlocking() {
    List<IpRange> ipRanges =
        List.of(
            IpRange.newBuilder()
                .setIpv4Range(IpV4Range.newBuilder().setStartIp(123L).setEndIp(789L).build())
                .build(),
            IpRange.newBuilder()
                .setIpv4Range(IpV4Range.newBuilder().setStartIp(234L).setEndIp(789L).build())
                .build(),
            IpRange.getDefaultInstance());

    List<ai.traceable.blocking.config.service.v1.IpRange> convertedIpRangesList =
        ipRangesConverter.convert(ipRanges);

    assertEquals(2, convertedIpRangesList.size());
    assertEquals(
        ai.traceable.blocking.config.service.v1.IpRange.newBuilder()
            .setIpv4Range(
                ai.traceable.blocking.config.service.v1.IpV4Range.newBuilder()
                    .setStartIp(123L)
                    .setEndIp(789L)
                    .build())
            .build(),
        convertedIpRangesList.get(0));
    assertEquals(
        ai.traceable.blocking.config.service.v1.IpRange.newBuilder()
            .setIpv4Range(
                ai.traceable.blocking.config.service.v1.IpV4Range.newBuilder()
                    .setStartIp(234L)
                    .setEndIp(789L)
                    .build())
            .build(),
        convertedIpRangesList.get(1));
  }
}
