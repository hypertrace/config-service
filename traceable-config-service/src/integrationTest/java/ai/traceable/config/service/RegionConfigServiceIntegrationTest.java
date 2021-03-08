package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.traceable.region.config.service.v1.GetRegionRequest;
import ai.traceable.region.config.service.v1.GetRegionResponse;
import ai.traceable.region.config.service.v1.GetRegionsRequest;
import ai.traceable.region.config.service.v1.GetRegionsResponse;
import ai.traceable.region.config.service.v1.Region;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import java.util.List;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class RegionConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static RegionConfigServiceBlockingStub regionConfigServiceStub;

  @BeforeAll
  static void init() {
    regionConfigServiceStub =
        RegionConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  public void getRegions() {
    GetRegionsResponse regionsResponse =
        regionConfigServiceStub.getRegions(GetRegionsRequest.getDefaultInstance());
    List<Region> regions = regionsResponse.getRegionList();
    assertEquals(248, regions.size());
    regions.forEach(
        region -> {
          assertFalse(region.getId().isEmpty());
          assertFalse(region.getName().isEmpty());
        });
  }

  @Test
  public void getRegion() {
    GetRegionsResponse regionsResponse =
        regionConfigServiceStub.getRegions(GetRegionsRequest.getDefaultInstance());
    List<Region> regions = regionsResponse.getRegionList();

    Region firstRegion = regions.get(0);
    String regionId = firstRegion.getId();

    GetRegionResponse regionResponse =
        regionConfigServiceStub.getRegion(GetRegionRequest.newBuilder().setId(regionId).build());
    Region region = regionResponse.getRegion();
    assertEquals(firstRegion, region);
  }
}
