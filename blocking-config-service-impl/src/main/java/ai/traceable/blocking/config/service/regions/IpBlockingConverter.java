package ai.traceable.blocking.config.service.regions;

import ai.traceable.blocking.config.service.v1.IpBlocking;
import ai.traceable.blocking.config.service.v1.IpRange;
import ai.traceable.blocking.config.service.v1.IpV4Range;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class IpBlockingConverter {
  IpBlocking convert(List<ai.traceable.region.config.service.v1.IpRange> ipRanges) {
    return IpBlocking.newBuilder()
        .addAllIpRange(
            ipRanges.stream()
                .map(this::convert)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList()))
        .build();
  }

  private Optional<IpRange> convert(ai.traceable.region.config.service.v1.IpRange ipRange) {
    switch (ipRange.getRangeCase()) {
      case IPV4_RANGE:
        return Optional.of(
            IpRange.newBuilder().setIpv4Range(this.convert(ipRange.getIpv4Range())).build());
      default:
        log.error("Unsupported ip range {}", ipRange);
        return Optional.empty();
    }
  }

  private IpV4Range convert(ai.traceable.region.config.service.v1.IpV4Range ipV4Range) {
    return IpV4Range.newBuilder()
        .setStartIp(ipV4Range.getStartIp())
        .setEndIp(ipV4Range.getEndIp())
        .build();
  }
}
