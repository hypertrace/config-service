package ai.traceable.region.config.service;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.region.config.service.regions.RegionStore;
import ai.traceable.region.config.service.rules.RulesManager;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetAllRegionRulesResponse;
import ai.traceable.region.config.service.v1.GetRegionRequest;
import ai.traceable.region.config.service.v1.GetRegionResponse;
import ai.traceable.region.config.service.v1.GetRegionsRequest;
import ai.traceable.region.config.service.v1.GetRegionsResponse;
import ai.traceable.region.config.service.v1.Region;
import ai.traceable.region.config.service.v1.RegionRule;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RegionConfigServiceImplTest {
  private static final String TENANT_ID = "tenant1";

  private RegionStore regionStore;
  private RulesManager rulesManager;

  private RegionConfigServiceImpl regionConfigService;

  @BeforeEach
  void setup() {
    regionStore = mock(RegionStore.class);
    rulesManager = mock(RulesManager.class);
    regionConfigService = new RegionConfigServiceImpl(regionStore, rulesManager);
  }

  @Nested
  class GetRegions {
    @Test
    void shouldGetCountries() {
      StreamObserver<GetRegionsResponse> responseObserver = mock(StreamObserver.class);
      List<Region> regions = List.of(Region.newBuilder().setId("id").setName("name").build());
      when(regionStore.getCountries()).thenReturn(regions);

      Runnable runnable =
          () ->
              regionConfigService.getRegions(
                  GetRegionsRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(GetRegionsResponse.newBuilder().addAllRegion(regions).build());
      verify(responseObserver, times(1)).onCompleted();
    }
  }

  @Nested
  class GetRegion {
    @Test
    void shouldGetRegion() {
      StreamObserver<GetRegionResponse> responseObserver = mock(StreamObserver.class);
      Region region = Region.newBuilder().setId("id").setName("name").build();
      when(regionStore.getRegion("id")).thenReturn(Optional.of(region));

      Runnable runnable =
          () ->
              regionConfigService.getRegion(
                  GetRegionRequest.newBuilder().setId("id").build(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(GetRegionResponse.newBuilder().setRegion(region).build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should return not found for missing region id in request")
    void should_error_noRegionId() {
      StreamObserver<GetRegionResponse> responseObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              regionConfigService.getRegion(
                  GetRegionRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("should return not found for invalid region id")
    void should_error_invalidRegionId() {
      StreamObserver<GetRegionResponse> responseObserver = mock(StreamObserver.class);
      when(regionStore.getRegion("id")).thenReturn(Optional.empty());

      Runnable runnable =
          () ->
              regionConfigService.getRegion(
                  GetRegionRequest.newBuilder().setId("id").build(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.NOT_FOUND));
    }
  }

  @Nested
  class GetAllRegionRules {
    @Test
    void shouldGetAllRegionRules() {
      RegionRule regionRule1 = RegionRule.newBuilder().setId("id-1").build();
      RegionRule regionRule2 = RegionRule.newBuilder().setId("id-2").build();
      when(rulesManager.getRegionRules()).thenReturn(List.of(regionRule1, regionRule2));

      StreamObserver<GetAllRegionRulesResponse> responseObserver = mock(StreamObserver.class);
      Runnable runnable =
          () ->
              regionConfigService.getAllRegionRules(
                  GetAllRegionRulesRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              GetAllRegionRulesResponse.newBuilder()
                  .addAllRule(List.of(regionRule1, regionRule2))
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }
  }
}
