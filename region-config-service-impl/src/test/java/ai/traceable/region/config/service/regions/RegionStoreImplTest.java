package ai.traceable.region.config.service.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.RegionIdentifier;
import com.google.common.base.Suppliers;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RegionStoreImplTest {
  private RegionStoreImpl regionStore;

  @BeforeEach
  void setup() {
    RegionBuilder regionBuilder = mock(RegionBuilder.class);
    RegionConverter regionConverter = mock(RegionConverter.class);

    when(regionConverter.convert(any(Region.class)))
        .thenReturn(ai.traceable.region.config.service.v1.Region.getDefaultInstance());
    when(regionBuilder.getLatestDataSupplier()).thenReturn(Suppliers.memoize(this::mockRegions));

    this.regionStore = new RegionStoreImpl(regionBuilder, regionConverter);
  }

  @Nested
  class GetCountries {
    @Test
    @DisplayName("should get all countries")
    void shouldGetAllCountries() {
      List<ai.traceable.region.config.service.v1.Region> regions =
          regionStore.getCountries(Collections.emptyList(), Collections.emptyList());
      assertEquals(2, regions.size());
    }

    @Test
    @DisplayName("should get countries based on region id")
    void shouldGetCountries_regionIds() {
      List<ai.traceable.region.config.service.v1.Region> regions =
          regionStore.getCountries(List.of("region-id-1"), Collections.emptyList());
      assertEquals(1, regions.size());
    }

    @Test
    @DisplayName("should get countries based on region identifier")
    void shouldGetCountries_regionIdentifiers() {
      List<ai.traceable.region.config.service.v1.Region> regions =
          regionStore.getCountries(
              Collections.emptyList(),
              List.of(RegionIdentifier.newBuilder().setCountryIsoCode("region-iso-1").build()));
      assertEquals(1, regions.size());
    }
  }

  @Nested
  class GetDetailedRegions {
    @Test
    @DisplayName("should get all regions")
    void shouldGetAllRegions() {
      List<DetailedRegion> regions =
          regionStore.getDetailedRegions(Collections.emptyList(), Collections.emptyList());
      assertEquals(2, regions.size());
    }

    @Test
    @DisplayName("should get regions based on region id")
    void shouldGetRegions_regionIds() {
      List<DetailedRegion> regions =
          regionStore.getDetailedRegions(List.of("region-id-1"), Collections.emptyList());
      assertEquals(1, regions.size());
    }

    @Test
    @DisplayName("should get regions based on region identifier")
    void shouldGetRegions_regionIdentifiers() {
      List<DetailedRegion> regions =
          regionStore.getDetailedRegions(
              Collections.emptyList(),
              List.of(RegionIdentifier.newBuilder().setCountryIsoCode("region-iso-1").build()));
      assertEquals(1, regions.size());
    }
  }

  @Nested
  class GetRegion {
    @Test
    @DisplayName("should get region")
    void shouldGetRegion() {
      Optional<ai.traceable.region.config.service.v1.Region> maybeRegion =
          regionStore.getRegion("region-id-2", RegionIdentifier.getDefaultInstance());
      assertTrue(maybeRegion.isPresent());

      maybeRegion =
          regionStore.getRegion(
              "", RegionIdentifier.newBuilder().setCountryIsoCode("region-iso-1").build());
      assertTrue(maybeRegion.isPresent());
    }

    @Test
    @DisplayName("should return empty, if region not present")
    void should_returnEmpty_regionNotPresent() {
      Optional<ai.traceable.region.config.service.v1.Region> maybeRegion =
          regionStore.getRegion("invalid-id", RegionIdentifier.getDefaultInstance());
      assertTrue(maybeRegion.isEmpty());

      maybeRegion =
          regionStore.getRegion(
              "", RegionIdentifier.newBuilder().setCountryIsoCode("invalid-iso").build());
      assertTrue(maybeRegion.isEmpty());
    }
  }

  private Map<String, Region> mockRegions() {
    return Map.of(
        "region-id-1",
        new Region(
            "region-id-1",
            "region-name-1",
            RegionType.COUNTRY,
            Collections.emptyList(),
            "region-iso-1"),
        "region-id-2",
        new Region(
            "region-id-2",
            "region-name-2",
            RegionType.COUNTRY,
            Collections.emptyList(),
            "region-iso-2"));
  }
}
