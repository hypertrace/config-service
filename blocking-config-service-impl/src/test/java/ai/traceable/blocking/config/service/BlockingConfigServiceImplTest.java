package ai.traceable.blocking.config.service;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.traceable.blocking.config.service.customsignature.CustomModsecBlockingManager;
import ai.traceable.blocking.config.service.regions.RegionBlockingManager;
import ai.traceable.blocking.config.service.safecrs.SafeCrsBlockingManager;
import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesResponse;
import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import ai.traceable.blocking.config.service.v1.SafeCrsBlockingRules;
import io.grpc.stub.StreamObserver;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockingConfigServiceImplTest {
  private static final String TENANT_ID = "tenant1";

  private RegionBlockingRules regionBlockingRules;
  private CustomModsecBlockingRules customModsecBlockingRules;
  private SafeCrsBlockingRules safeCrsBlockingRules;

  private BlockingConfigServiceImpl blockingConfigService;
  private String hash1 = "hash1";
  private String hash2 = "hash2";

  @BeforeEach
  void setup() {
    RegionBlockingManager regionBlockingManager = mock(RegionBlockingManager.class);
    CustomModsecBlockingManager customModsecBlockingManager =
        mock(CustomModsecBlockingManager.class);
    SafeCrsBlockingManager safeCrsBlockingManager = mock(SafeCrsBlockingManager.class);

    this.regionBlockingRules = RegionBlockingRules.newBuilder().setHash(hash2).build();
    this.customModsecBlockingRules = CustomModsecBlockingRules.newBuilder().setHash(hash2).build();
    this.safeCrsBlockingRules = SafeCrsBlockingRules.newBuilder().setHash(hash2).build();

    doReturn(regionBlockingRules).when(regionBlockingManager).getEnabledBlockingRules(hash1);
    doReturn(customModsecBlockingRules)
        .when(customModsecBlockingManager)
        .getEnabledBlockingRules(hash1);
    doReturn(safeCrsBlockingRules).when(safeCrsBlockingManager).getBlockingRules(hash1);

    this.blockingConfigService =
        new BlockingConfigServiceImpl(
            regionBlockingManager, customModsecBlockingManager, safeCrsBlockingManager);
  }

  @Test
  void testResponse() {
    StreamObserver<GetBlockingRulesResponse> responseObserver = mock(StreamObserver.class);

    Runnable runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setRegionBlockingRulesHash(hash1)
                    .setCustomModsecBlockingRulesHash(hash1)
                    .setSafeCrsBlockingRulesHash(hash1)
                    .build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(
            GetBlockingRulesResponse.newBuilder()
                .setRegionBlockingRules(regionBlockingRules)
                .setCustomModsecBlockingRules(customModsecBlockingRules)
                .setSafeCrsBlockingRules(safeCrsBlockingRules)
                .build());

    // exception for region blocking rules..
    runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setRegionBlockingRulesHash("")
                    .setCustomModsecBlockingRulesHash(hash1)
                    .setSafeCrsBlockingRulesHash(hash1)
                    .build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(
            GetBlockingRulesResponse.newBuilder()
                .setCustomModsecBlockingRules(customModsecBlockingRules)
                .setSafeCrsBlockingRules(safeCrsBlockingRules)
                .build());

    // exception for custom signature blocking rules..
    runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setRegionBlockingRulesHash(hash1)
                    .setCustomModsecBlockingRulesHash("")
                    .setSafeCrsBlockingRulesHash(hash1)
                    .build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(
            GetBlockingRulesResponse.newBuilder()
                .setRegionBlockingRules(regionBlockingRules)
                .setSafeCrsBlockingRules(safeCrsBlockingRules)
                .build());

    // exception for safe crs rules..
    runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setRegionBlockingRulesHash(hash1)
                    .setCustomModsecBlockingRulesHash(hash1)
                    .setSafeCrsBlockingRulesHash("")
                    .build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(
            GetBlockingRulesResponse.newBuilder()
                .setRegionBlockingRules(regionBlockingRules)
                .setCustomModsecBlockingRules(customModsecBlockingRules)
                .build());

    verify(responseObserver, times(4)).onCompleted();
  }
}
