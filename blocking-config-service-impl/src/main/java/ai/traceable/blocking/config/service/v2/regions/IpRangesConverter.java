package ai.traceable.blocking.config.service.v2.regions;

import ai.traceable.blocking.config.service.v2.IpRange;
import ai.traceable.blocking.config.service.v2.IpV4Range;
import ai.traceable.region.config.service.v1.IpRange.RangeCase;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class IpRangesConverter {
  List<IpRange> convert(List<ai.traceable.region.config.service.v1.IpRange> ipRanges) {
    return ipRanges.stream()
        .map(this::convert)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<IpRange> convert(ai.traceable.region.config.service.v1.IpRange ipRange) {
    if (ipRange.getRangeCase() == RangeCase.IPV4_RANGE) {
      return Optional.of(
          IpRange.newBuilder().setIpv4Range(this.convert(ipRange.getIpv4Range())).build());
    }
    log.error("Unsupported ip range {}", ipRange);
    return Optional.empty();
  }

  private IpV4Range convert(ai.traceable.region.config.service.v1.IpV4Range ipV4Range) {
    return IpV4Range.newBuilder()
        .setStartIp((int) ipV4Range.getStartIp())
        .setEndIp((int) ipV4Range.getEndIp())
        .build();
  }
}
