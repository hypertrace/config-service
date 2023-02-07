package ai.traceable.region.config.service.regions;

import java.util.List;
import lombok.Value;

@Value
public class Region {
  String id;
  String name;
  RegionType type;
  List<IpV4Range> ipV4Ranges;
  String isoCode;
}
