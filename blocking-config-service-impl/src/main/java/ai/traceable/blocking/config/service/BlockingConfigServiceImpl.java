package ai.traceable.blocking.config.service;

import ai.traceable.blocking.config.service.blockingmodsec.ModsecBlockingManager;
import ai.traceable.blocking.config.service.blockingpolicy.BlockingPolicyConfigurationManager;
import ai.traceable.blocking.config.service.customsignature.CustomModsecBlockingManager;
import ai.traceable.blocking.config.service.regions.RegionBlockingManager;
import ai.traceable.blocking.config.service.v1.BlockingConfigServiceGrpc.BlockingConfigServiceImplBase;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class BlockingConfigServiceImpl extends BlockingConfigServiceImplBase {
  private final RegionBlockingManager regionBlockingManager;
  private final CustomModsecBlockingManager customModsecBlockingManager;
  private final ModsecBlockingManager modsecBlockingManager;
  private final BlockingPolicyConfigurationManager blockingPolicyConfigurationManager;

  @Inject
  public BlockingConfigServiceImpl(
      RegionBlockingManager regionBlockingManager,
      CustomModsecBlockingManager customModsecBlockingManager,
      ModsecBlockingManager modsecBlockingManager,
      BlockingPolicyConfigurationManager blockingPolicyConfigurationManager) {
    this.regionBlockingManager = regionBlockingManager;
    this.customModsecBlockingManager = customModsecBlockingManager;
    this.modsecBlockingManager = modsecBlockingManager;
    this.blockingPolicyConfigurationManager = blockingPolicyConfigurationManager;
  }

  @Override
  public void getBlockingRules(
      GetBlockingRulesRequest request, StreamObserver<GetBlockingRulesResponse> responseObserver) {

    GetBlockingRulesResponse.Builder responseBuilder = GetBlockingRulesResponse.newBuilder();

    try {
      RequestContext requestContext = RequestContext.CURRENT.get();

      responseBuilder.setRegionBlockingRules(
          regionBlockingManager.getEnabledBlockingRules(
              requestContext, request.getRegionBlockingRulesHash()));

      responseBuilder.setCustomModsecBlockingRules(
          customModsecBlockingManager.getEnabledBlockingRules(
              requestContext, request.getCustomModsecBlockingRulesHash()));

      responseBuilder.setSafeCrsBlockingRules(
          modsecBlockingManager.getBlockingRules(request.getSafeCrsBlockingRulesHash()));

      responseBuilder.setBlockingPolicyConfiguration(
          blockingPolicyConfigurationManager.getBlockingPolicyConfiguration(
              requestContext, request.getBlockingPolicyConfigurationHash()));

      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.error("Get Blocking Rules RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
