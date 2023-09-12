package ai.traceable.region.config.service.regions;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode
public class IpV4Range {
  long startIp;
  long endIp;

  public long getIpsCount() {
    return endIp - startIp + 1;
  }
}
