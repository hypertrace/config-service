package ai.traceable.region.config.service.regions;

class RegionConverter {
  ai.traceable.region.config.service.v1.Region convert(Region region) {
    return ai.traceable.region.config.service.v1.Region.newBuilder()
        .setId(region.getId())
        .setName(region.getName())
        .build();
  }
}
