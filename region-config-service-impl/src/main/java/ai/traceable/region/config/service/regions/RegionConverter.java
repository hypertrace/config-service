package ai.traceable.region.config.service.regions;

import ai.traceable.region.config.service.v1.DetailedRegion;
import com.google.inject.Inject;

class RegionConverter {
  private final IpRangeConverter ipRangeConverter;

  @Inject
  RegionConverter(IpRangeConverter ipRangeConverter) {
    this.ipRangeConverter = ipRangeConverter;
  }

  ai.traceable.region.config.service.v1.Region convert(Region region) {
    return ai.traceable.region.config.service.v1.Region.newBuilder()
        .setId(region.getId())
        .setName(region.getName())
        .build();
  }

  DetailedRegion convertToDetailedRegion(Region region) {
    return DetailedRegion.newBuilder()
        .setId(region.getId())
        .setName(region.getName())
        .addAllIpRange(this.ipRangeConverter.convert(region.getIpV4Ranges()))
        .build();
  }
}
