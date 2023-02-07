package ai.traceable.region.config.service.regions;

import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.Region;
import ai.traceable.region.config.service.v1.RegionIdentifier;
import java.util.List;
import java.util.Optional;

public interface RegionStore {
  List<ai.traceable.region.config.service.v1.Region> getCountries(
      List<String> ids, List<RegionIdentifier> regionIdentifiers);

  List<DetailedRegion> getDetailedRegions(
      List<String> ids, List<RegionIdentifier> regionIdentifiers);

  Optional<Region> getRegion(String id, RegionIdentifier regionIdentifier);
}
