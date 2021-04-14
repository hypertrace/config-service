package ai.traceable.blocking.config.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.regions.RegionBlockingManager;
import ai.traceable.blocking.config.service.v1.BlockingRules;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesResponse;
import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import io.grpc.stub.StreamObserver;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockingConfigServiceImplTest {
  private static final String TENANT_ID = "tenant1";

  private HashGenerator hashGenerator;
  private RegionBlockingManager regionBlockingManager;

  private BlockingConfigServiceImpl blockingConfigService;

  @BeforeEach
  void setup() {
    this.hashGenerator = mock(HashGenerator.class);
    this.regionBlockingManager = mock(RegionBlockingManager.class);

    this.blockingConfigService =
        new BlockingConfigServiceImpl(this.hashGenerator, this.regionBlockingManager);
  }

  @Test
  void shouldReturnResponse_hashChanged() {
    StreamObserver<GetBlockingRulesResponse> responseObserver = mock(StreamObserver.class);
    RegionBlockingRules regionBlockingRules = RegionBlockingRules.getDefaultInstance();
    BlockingRules blockingRules =
        BlockingRules.newBuilder().setRegionBlockingRules(regionBlockingRules).build();
    when(this.regionBlockingManager.getBlockingRules()).thenReturn(regionBlockingRules);
    when(this.hashGenerator.generate(blockingRules)).thenReturn("new-hash");

    Runnable runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder().setOmitIfMatchesHash("old-hash").build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    verify(responseObserver, times(1))
        .onNext(
            GetBlockingRulesResponse.newBuilder()
                .setBlockingRules(blockingRules)
                .setHash("new-hash")
                .build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void shouldReturnResponse_hashUnchanged() {
    StreamObserver<GetBlockingRulesResponse> responseObserver = mock(StreamObserver.class);
    RegionBlockingRules regionBlockingRules = RegionBlockingRules.getDefaultInstance();
    BlockingRules blockingRules =
        BlockingRules.newBuilder().setRegionBlockingRules(regionBlockingRules).build();
    when(this.regionBlockingManager.getBlockingRules()).thenReturn(regionBlockingRules);
    when(this.hashGenerator.generate(blockingRules)).thenReturn("old-hash");

    Runnable runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder().setOmitIfMatchesHash("old-hash").build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    verify(responseObserver, times(1))
        .onNext(GetBlockingRulesResponse.newBuilder().setHash("old-hash").build());
    verify(responseObserver, times(1)).onCompleted();
  }
}
