package ai.traceable.region.config.service.regions;

import ai.traceable.region.config.service.v1.Country;
import ai.traceable.region.config.service.v1.DetailedRegion;
import com.google.inject.Inject;

class RegionConverter {
  private final IpRangeConverter ipRangeConverter;

  @Inject
  RegionConverter(IpRangeConverter ipRangeConverter) {
    this.ipRangeConverter = ipRangeConverter;
  }

  ai.traceable.region.config.service.v1.Region convert(Region region) {

    ai.traceable.region.config.service.v1.Region.Builder builder =
        ai.traceable.region.config.service.v1.Region.newBuilder();

    if (region.getType().equals(RegionType.COUNTRY)) {
      builder.setCountry(
          Country.newBuilder().setName(region.getName()).setIsoCode(region.getIsoCode()).build());
    }

    return builder.setId(region.getId()).setName(region.getName()).build();
  }

  DetailedRegion convertToDetailedRegion(Region region) {
    return DetailedRegion.newBuilder()
        .setId(region.getId())
        .setName(region.getName())
        .setRegion(convert(region))
        .addAllIpRange(this.ipRangeConverter.convert(region.getIpV4Ranges()))
        .build();
  }
}
