package ai.traceable.region.config.service.regions;

import ai.traceable.config.utils.refresh.FileRefreshConfig;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.RegionIdentifier;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RegionStoreImpl implements RegionStore {
  private final RegionConverter regionConverter;
  private final Supplier<Map<String, Region>> regionIdToRegionMapSupplier;
  private final Supplier<Map<RegionIdentifier, Region>> regionIdentifierToRegionMapSupplier;

  RegionStoreImpl(
      RegionBuilder regionBuilder, RegionConverter regionConverter, FileRefreshConfig dataConfig) {
    this.regionConverter = regionConverter;
    this.regionIdToRegionMapSupplier = regionBuilder.getLatestDataSupplier(dataConfig);
    this.regionIdentifierToRegionMapSupplier = populateRegionIdentifierToRegionMapSupplier();
  }

  @Override
  public List<ai.traceable.region.config.service.v1.Region> getCountries(
      List<String> ids, List<RegionIdentifier> regionIdentifiers) {
    return getRegions(ids, regionIdentifiers).stream()
        .filter(region -> RegionType.COUNTRY.equals(region.getType()))
        .sorted(Comparator.comparing(Region::getName))
        .map(regionConverter::convert)
        .collect(Collectors.toList());
  }

  @Override
  public List<DetailedRegion> getDetailedRegions(
      List<String> ids, List<RegionIdentifier> regionIdentifiers) {
    return getRegions(ids, regionIdentifiers).stream()
        .map(regionConverter::convertToDetailedRegion)
        .collect(Collectors.toList());
  }

  @Override
  public Optional<ai.traceable.region.config.service.v1.Region> getRegion(
      String id, RegionIdentifier regionIdentifier) {
    Optional<Region> regionOptional =
        !id.isEmpty()
            ? Optional.ofNullable(regionIdToRegionMapSupplier.get().get(id))
            : Optional.ofNullable(regionIdentifierToRegionMapSupplier.get().get(regionIdentifier));
    return regionOptional.map(regionConverter::convert);
  }

  private List<Region> getRegions(List<String> ids, List<RegionIdentifier> regionIdentifiers) {
    if (!ids.isEmpty()) {
      Map<String, Region> regionIdToRegionMap = regionIdToRegionMapSupplier.get();
      return ids.stream()
          .map(regionIdToRegionMap::get)
          .filter(Objects::nonNull)
          .collect(Collectors.toUnmodifiableList());
    } else {
      Map<RegionIdentifier, Region> regionIdentifierToRegionMap =
          regionIdentifierToRegionMapSupplier.get();
      return regionIdentifiers.isEmpty()
          ? List.copyOf(regionIdentifierToRegionMap.values())
          : regionIdentifiers.stream()
              .map(regionIdentifierToRegionMap::get)
              .filter(Objects::nonNull)
              .collect(Collectors.toUnmodifiableList());
    }
  }

  private Supplier<Map<RegionIdentifier, Region>> populateRegionIdentifierToRegionMapSupplier() {
    return com.google.common.base.Suppliers.memoize(
        () ->
            regionIdToRegionMapSupplier.get().values().stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        region ->
                            RegionIdentifier.newBuilder()
                                .setCountryIsoCode(region.getIsoCode())
                                .build(),
                        Function.identity())));
  }
}
