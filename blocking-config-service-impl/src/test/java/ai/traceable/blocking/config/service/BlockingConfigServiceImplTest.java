package ai.traceable.blocking.config.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.traceable.blocking.config.service.blockingmodsec.ModsecBlockingManager;
import ai.traceable.blocking.config.service.blockingpolicy.BlockingPolicyConfigurationManager;
import ai.traceable.blocking.config.service.customsignature.CustomModsecBlockingManager;
import ai.traceable.blocking.config.service.regions.RegionBlockingManager;
import ai.traceable.blocking.config.service.v1.BlockingPolicyConfiguration;
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
  private SafeCrsBlockingRules modsecCrsBlockingRules;
  private BlockingPolicyConfiguration blockingPolicyConfiguration;

  private BlockingConfigServiceImpl blockingConfigService;
  private String hash1 = "hash1";
  private String hash2 = "hash2";

  @BeforeEach
  void setup() {
    RegionBlockingManager regionBlockingManager = mock(RegionBlockingManager.class);
    CustomModsecBlockingManager customModsecBlockingManager =
        mock(CustomModsecBlockingManager.class);
    ModsecBlockingManager modsecBlockingManager = mock(ModsecBlockingManager.class);
    BlockingPolicyConfigurationManager blockingPolicyConfigurationManager =
        mock(BlockingPolicyConfigurationManager.class);

    this.regionBlockingRules = RegionBlockingRules.newBuilder().setHash(hash2).build();
    this.customModsecBlockingRules = CustomModsecBlockingRules.newBuilder().setHash(hash2).build();
    this.modsecCrsBlockingRules = SafeCrsBlockingRules.newBuilder().setHash(hash2).build();
    this.blockingPolicyConfiguration =
        BlockingPolicyConfiguration.newBuilder().setHash(hash2).build();

    doReturn(regionBlockingRules).when(regionBlockingManager).getEnabledBlockingRules(hash1);
    doReturn(customModsecBlockingRules)
        .when(customModsecBlockingManager)
        .getEnabledBlockingRules(hash1);
    doReturn(modsecCrsBlockingRules).when(modsecBlockingManager).getBlockingRules(hash1);
    doReturn(blockingPolicyConfiguration)
        .when(blockingPolicyConfigurationManager)
        .getBlockingPolicyConfiguration(hash1);

    this.blockingConfigService =
        new BlockingConfigServiceImpl(
            regionBlockingManager,
            customModsecBlockingManager,
            modsecBlockingManager,
            blockingPolicyConfigurationManager);
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
                    .setBlockingPolicyConfigurationHash(hash1)
                    .build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(
            GetBlockingRulesResponse.newBuilder()
                .setRegionBlockingRules(regionBlockingRules)
                .setCustomModsecBlockingRules(customModsecBlockingRules)
                .setSafeCrsBlockingRules(modsecCrsBlockingRules)
                .setBlockingPolicyConfiguration(blockingPolicyConfiguration)
                .build());

    // exception for region blocking rules
    runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setRegionBlockingRulesHash("")
                    .setCustomModsecBlockingRulesHash(hash1)
                    .setSafeCrsBlockingRulesHash(hash1)
                    .setBlockingPolicyConfigurationHash(hash1)
                    .build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    // exception for custom signature blocking rules
    runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setRegionBlockingRulesHash(hash1)
                    .setCustomModsecBlockingRulesHash("")
                    .setSafeCrsBlockingRulesHash(hash1)
                    .setBlockingPolicyConfigurationHash(hash1)
                    .build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    // exception for safe crs rules
    runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setRegionBlockingRulesHash(hash1)
                    .setCustomModsecBlockingRulesHash(hash1)
                    .setSafeCrsBlockingRulesHash("")
                    .setBlockingPolicyConfigurationHash(hash1)
                    .build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    // exception for blocking policy configuration
    runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setRegionBlockingRulesHash(hash1)
                    .setCustomModsecBlockingRulesHash(hash1)
                    .setSafeCrsBlockingRulesHash(hash1)
                    .setBlockingPolicyConfigurationHash("")
                    .build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    verify(responseObserver, times(4)).onError(any(RuntimeException.class));
    verify(responseObserver, times(1)).onCompleted();
  }
}
