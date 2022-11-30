package ai.traceable.region.config.service.regions;

import ai.traceable.config.utils.refresh.FileRefreshConfig;
import ai.traceable.region.config.service.v1.DetailedRegion;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RegionStoreImpl implements RegionStore {
  private final RegionConverter regionConverter;
  private final Supplier<Map<String, Region>> regionIdToRegionMapSupplier;

  RegionStoreImpl(
      RegionBuilder regionBuilder, RegionConverter regionConverter, FileRefreshConfig dataConfig) {
    this.regionConverter = regionConverter;
    this.regionIdToRegionMapSupplier = regionBuilder.getLatestDataSupplier(dataConfig);
  }

  @Override
  public List<ai.traceable.region.config.service.v1.Region> getCountries(List<String> ids) {
    return getRegions(ids).stream()
        .filter(region -> RegionType.COUNTRY.equals(region.getType()))
        .sorted(Comparator.comparing(Region::getName))
        .map(this.regionConverter::convert)
        .collect(Collectors.toList());
  }

  @Override
  public List<DetailedRegion> getDetailedRegions(List<String> ids) {
    return getRegions(ids).stream()
        .map(this.regionConverter::convertToDetailedRegion)
        .collect(Collectors.toList());
  }

  @Override
  public Optional<ai.traceable.region.config.service.v1.Region> getRegion(String id) {
    return Optional.ofNullable(regionIdToRegionMapSupplier.get().get(id))
        .map(this.regionConverter::convert);
  }

  private List<Region> getRegions(List<String> ids) {
    Map<String, Region> regionIdToRegionMap = regionIdToRegionMapSupplier.get();
    return ids.isEmpty()
        ? List.copyOf(regionIdToRegionMap.values())
        : ids.stream()
            .map(regionIdToRegionMap::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());
  }
}
