package ai.traceable.blocking.config.service;

import ai.traceable.blocking.config.service.blockingmodsec.ModsecBlockingManager;
import ai.traceable.blocking.config.service.blockingpolicy.BlockingPolicyConfigurationManager;
import ai.traceable.blocking.config.service.customsignature.CustomModsecBlockingManager;
import ai.traceable.blocking.config.service.entity.EntityFetcher;
import ai.traceable.blocking.config.service.regions.RegionBlockingManager;
import ai.traceable.blocking.config.service.v1.BlockingConfigServiceGrpc.BlockingConfigServiceImplBase;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class BlockingConfigServiceImpl extends BlockingConfigServiceImplBase {
  private final RegionBlockingManager regionBlockingManager;
  private final CustomModsecBlockingManager customModsecBlockingManager;
  private final ModsecBlockingManager modsecBlockingManager;
  private final BlockingPolicyConfigurationManager blockingPolicyConfigurationManager;
  private final EntityFetcher entityFetcher;

  @Inject
  public BlockingConfigServiceImpl(
      RegionBlockingManager regionBlockingManager,
      CustomModsecBlockingManager customModsecBlockingManager,
      ModsecBlockingManager modsecBlockingManager,
      BlockingPolicyConfigurationManager blockingPolicyConfigurationManager,
      EntityFetcher entityFetcher) {
    this.regionBlockingManager = regionBlockingManager;
    this.customModsecBlockingManager = customModsecBlockingManager;
    this.modsecBlockingManager = modsecBlockingManager;
    this.blockingPolicyConfigurationManager = blockingPolicyConfigurationManager;
    this.entityFetcher = entityFetcher;
  }

  @Override
  public void getBlockingRules(
      GetBlockingRulesRequest request, StreamObserver<GetBlockingRulesResponse> responseObserver) {

    GetBlockingRulesResponse.Builder responseBuilder = GetBlockingRulesResponse.newBuilder();
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();

      Optional<String> environmentId =
          entityFetcher.getEnvironmentId(requestContext, request.getEnvironment());

      responseBuilder.setRegionBlockingRules(
          regionBlockingManager.getEnabledBlockingRules(
              requestContext, request.getRegionBlockingRulesHash(), environmentId));

      responseBuilder.setCustomModsecBlockingRules(
          customModsecBlockingManager.getEnabledBlockingRules(
              requestContext, request.getCustomModsecBlockingRulesHash(), environmentId));

      responseBuilder.setSafeCrsBlockingRules(
          modsecBlockingManager.getBlockingRules(request.getSafeCrsBlockingRulesHash()));

      responseBuilder.setBlockingPolicyConfiguration(
          blockingPolicyConfigurationManager.getBlockingPolicyConfiguration(
              requestContext, request.getBlockingPolicyConfigurationHash(), environmentId));

      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException | ExecutionException e) {
      log.error("Get Blocking Rules RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
