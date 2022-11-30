package ai.traceable.blocking.config.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.traceable.blocking.config.service.blockingmodsec.ModsecBlockingManager;
import ai.traceable.blocking.config.service.blockingpolicy.BlockingPolicyConfigurationManager;
import ai.traceable.blocking.config.service.customsignature.CustomModsecBlockingManager;
import ai.traceable.blocking.config.service.entity.EntityFetcher;
import ai.traceable.blocking.config.service.iptype.IpTypeBlockingManager;
import ai.traceable.blocking.config.service.regions.RegionBlockingManager;
import ai.traceable.blocking.config.service.v1.BlockingPolicyConfiguration;
import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesResponse;
import ai.traceable.blocking.config.service.v1.IpTypeBlockingRules;
import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import ai.traceable.blocking.config.service.v1.SafeCrsBlockingRules;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockingConfigServiceImplTest {
  private static final String TENANT_ID = "tenant1";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private RegionBlockingRules regionBlockingRules;
  private CustomModsecBlockingRules customModsecBlockingRules;
  private SafeCrsBlockingRules modsecCrsBlockingRules;
  private IpTypeBlockingRules ipTypeBlockingRules;
  private BlockingPolicyConfiguration blockingPolicyConfiguration;

  private BlockingConfigServiceImpl blockingConfigService;
  private final String hash1 = "hash1";
  private final String hash2 = "hash2";
  private final String environment = "env";
  private final Optional<String> environmentId = Optional.of("env-id");

  @BeforeEach
  void setup() throws ExecutionException {
    RegionBlockingManager regionBlockingManager = mock(RegionBlockingManager.class);
    CustomModsecBlockingManager customModsecBlockingManager =
        mock(CustomModsecBlockingManager.class);
    ModsecBlockingManager modsecBlockingManager = mock(ModsecBlockingManager.class);
    IpTypeBlockingManager ipTypeBlockingManager = mock(IpTypeBlockingManager.class);
    BlockingPolicyConfigurationManager blockingPolicyConfigurationManager =
        mock(BlockingPolicyConfigurationManager.class);
    EntityFetcher entityFetcher = mock(EntityFetcher.class);

    this.regionBlockingRules = RegionBlockingRules.newBuilder().setHash(hash2).build();
    this.customModsecBlockingRules = CustomModsecBlockingRules.newBuilder().setHash(hash2).build();
    this.modsecCrsBlockingRules = SafeCrsBlockingRules.newBuilder().setHash(hash2).build();
    this.ipTypeBlockingRules = IpTypeBlockingRules.newBuilder().setHash(hash2).build();
    this.blockingPolicyConfiguration =
        BlockingPolicyConfiguration.newBuilder().setHash(hash2).build();

    doReturn(environmentId).when(entityFetcher).getEnvironmentId(REQUEST_CONTEXT, environment);

    doReturn(regionBlockingRules)
        .when(regionBlockingManager)
        .getEnabledBlockingRules(REQUEST_CONTEXT, hash1, environmentId);
    doReturn(customModsecBlockingRules)
        .when(customModsecBlockingManager)
        .getEnabledBlockingRules(REQUEST_CONTEXT, hash1, environmentId);
    doReturn(modsecCrsBlockingRules).when(modsecBlockingManager).getBlockingRules(hash1);
    doReturn(ipTypeBlockingRules)
        .when(ipTypeBlockingManager)
        .getEnabledBlockingRules(REQUEST_CONTEXT, hash1, environmentId);
    doReturn(blockingPolicyConfiguration)
        .when(blockingPolicyConfigurationManager)
        .getBlockingPolicyConfiguration(REQUEST_CONTEXT, hash1, environmentId);

    this.blockingConfigService =
        new BlockingConfigServiceImpl(
            regionBlockingManager,
            customModsecBlockingManager,
            modsecBlockingManager,
            ipTypeBlockingManager,
            blockingPolicyConfigurationManager,
            entityFetcher);
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
                    .setIpTypeBlockingRulesHash(hash1)
                    .setBlockingPolicyConfigurationHash(hash1)
                    .setEnvironment(environment)
                    .build(),
                responseObserver);

    REQUEST_CONTEXT.run(runnable);
    verify(responseObserver, times(1))
        .onNext(
            GetBlockingRulesResponse.newBuilder()
                .setRegionBlockingRules(regionBlockingRules)
                .setCustomModsecBlockingRules(customModsecBlockingRules)
                .setSafeCrsBlockingRules(modsecCrsBlockingRules)
                .setIpTypeBlockingRules(ipTypeBlockingRules)
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
                    .setIpTypeBlockingRulesHash(hash1)
                    .setBlockingPolicyConfigurationHash(hash1)
                    .build(),
                responseObserver);
    REQUEST_CONTEXT.run(runnable);

    // exception for custom signature blocking rules
    runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setRegionBlockingRulesHash(hash1)
                    .setCustomModsecBlockingRulesHash("")
                    .setSafeCrsBlockingRulesHash(hash1)
                    .setIpTypeBlockingRulesHash(hash1)
                    .setBlockingPolicyConfigurationHash(hash1)
                    .build(),
                responseObserver);
    REQUEST_CONTEXT.run(runnable);

    // exception for safe crs rules
    runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setRegionBlockingRulesHash(hash1)
                    .setCustomModsecBlockingRulesHash(hash1)
                    .setSafeCrsBlockingRulesHash("")
                    .setIpTypeBlockingRulesHash(hash1)
                    .setBlockingPolicyConfigurationHash(hash1)
                    .build(),
                responseObserver);
    REQUEST_CONTEXT.run(runnable);

    // exception for ip type blocking rules
    runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setRegionBlockingRulesHash(hash1)
                    .setCustomModsecBlockingRulesHash(hash1)
                    .setSafeCrsBlockingRulesHash(hash1)
                    .setIpTypeBlockingRulesHash("")
                    .setBlockingPolicyConfigurationHash(hash1)
                    .build(),
                responseObserver);
    REQUEST_CONTEXT.run(runnable);

    // exception for blocking policy configuration
    runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setRegionBlockingRulesHash(hash1)
                    .setCustomModsecBlockingRulesHash(hash1)
                    .setSafeCrsBlockingRulesHash(hash1)
                    .setIpTypeBlockingRulesHash(hash1)
                    .setBlockingPolicyConfigurationHash("")
                    .build(),
                responseObserver);
    REQUEST_CONTEXT.run(runnable);

    verify(responseObserver, times(5)).onError(any(RuntimeException.class));
    verify(responseObserver, times(1)).onCompleted();
  }
}
