package ai.traceable.region.config.service.regions;

import lombok.Value;

@Value
public class IpV4Range {
  long startIp;
  long endIp;
}
