package ai.traceable.region.config.service.regions;

import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.Region;
import java.util.List;
import java.util.Optional;

public interface RegionStore {
  List<ai.traceable.region.config.service.v1.Region> getCountries(List<String> ids);

  List<DetailedRegion> getDetailedRegions(List<String> ids);

  Optional<Region> getRegion(String id);
}
