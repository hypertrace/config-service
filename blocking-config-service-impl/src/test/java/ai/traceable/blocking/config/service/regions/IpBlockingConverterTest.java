package ai.traceable.blocking.config.service.regions;

import static org.junit.Assert.assertEquals;

import ai.traceable.blocking.config.service.v1.IpBlocking;
import ai.traceable.region.config.service.v1.IpRange;
import ai.traceable.region.config.service.v1.IpV4Range;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IpBlockingConverterTest {
  private IpBlockingConverter ipBlockingConverter;

  @BeforeEach
  void setup() {
    this.ipBlockingConverter = new IpBlockingConverter();
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

    assertEquals(
        IpBlocking.newBuilder()
            .addIpRange(
                ai.traceable.blocking.config.service.v1.IpRange.newBuilder()
                    .setIpv4Range(
                        ai.traceable.blocking.config.service.v1.IpV4Range.newBuilder()
                            .setStartIp(123L)
                            .setEndIp(789L)
                            .build())
                    .build())
            .addIpRange(
                ai.traceable.blocking.config.service.v1.IpRange.newBuilder()
                    .setIpv4Range(
                        ai.traceable.blocking.config.service.v1.IpV4Range.newBuilder()
                            .setStartIp(234L)
                            .setEndIp(789L)
                            .build())
                    .build())
            .build(),
        this.ipBlockingConverter.convert(ipRanges));
  }
}
