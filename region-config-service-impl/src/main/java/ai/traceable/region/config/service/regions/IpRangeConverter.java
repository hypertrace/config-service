package ai.traceable.region.config.service.regions;

import ai.traceable.region.config.service.v1.IpRange;
import java.util.List;
import java.util.stream.Collectors;

class IpRangeConverter {
  List<IpRange> convert(List<IpV4Range> ipV4Ranges) {
    return ipV4Ranges.stream().map(this::convert).collect(Collectors.toList());
  }

  private IpRange convert(IpV4Range ipV4Range) {
    return IpRange.newBuilder()
        .setIpv4Range(
            ai.traceable.region.config.service.v1.IpV4Range.newBuilder()
                .setStartIp(ipV4Range.getStartIp())
                .setEndIp(ipV4Range.getEndIp()))
        .build();
  }
}
