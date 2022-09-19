package ai.traceable.region.config.service.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.region.config.service.v1.DetailedRegion;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RegionStoreImplTest {
  private RegionBuilder regionBuilder;
  private RegionConverter regionConverter;
  private String countriesDataPath;

  private RegionStoreImpl regionStore;

  @BeforeEach
  void setup() {
    regionBuilder = mock(RegionBuilder.class);
    regionConverter = mock(RegionConverter.class);
    countriesDataPath = "/path/to/csv";

    when(regionConverter.convert(any(Region.class)))
        .thenReturn(ai.traceable.region.config.service.v1.Region.getDefaultInstance());
    when(regionBuilder.buildRegions(any(String.class))).thenReturn(mockRegions());

    this.regionStore = new RegionStoreImpl(regionBuilder, regionConverter, countriesDataPath);
  }

  @Nested
  class GetCountries {
    @Test
    @DisplayName("should get all countries")
    void shouldGetAllCountries() {
      List<ai.traceable.region.config.service.v1.Region> regions =
          regionStore.getCountries(Collections.emptyList());
      assertEquals(2, regions.size());
    }

    @Test
    @DisplayName("should get countries based on region id")
    void shouldGetCountries_regionIds() {
      List<ai.traceable.region.config.service.v1.Region> regions =
          regionStore.getCountries(List.of("region-id-1"));
      assertEquals(1, regions.size());
    }
  }

  @Nested
  class GetDetailedRegions {
    @Test
    @DisplayName("should get all regions")
    void shouldGetAllRegions() {
      List<DetailedRegion> regions = regionStore.getDetailedRegions(Collections.emptyList());
      assertEquals(2, regions.size());
    }

    @Test
    @DisplayName("should get regions based on region id")
    void shouldGetRegions_regionIds() {
      List<DetailedRegion> regions = regionStore.getDetailedRegions(List.of("region-id-1"));
      assertEquals(1, regions.size());
    }
  }

  @Nested
  class GetRegion {
    @Test
    @DisplayName("should get region")
    void shouldGetRegion() {
      Optional<ai.traceable.region.config.service.v1.Region> maybeRegion =
          regionStore.getRegion("region-id-2");
      assertTrue(maybeRegion.isPresent());
    }

    @Test
    @DisplayName("should return empty, if region not present")
    void should_returnEmpty_regionNotPresent() {
      Optional<ai.traceable.region.config.service.v1.Region> maybeRegion =
          regionStore.getRegion("invalid-id");
      assertTrue(maybeRegion.isEmpty());
    }
  }

  private Map<String, Region> mockRegions() {
    return Map.of(
        "region-id-1",
        new Region("region-id-1", "region-name-1", RegionType.COUNTRY, Collections.emptyList()),
        "region-id-2",
        new Region("region-id-2", "region-name-2", RegionType.COUNTRY, Collections.emptyList()));
  }
}
