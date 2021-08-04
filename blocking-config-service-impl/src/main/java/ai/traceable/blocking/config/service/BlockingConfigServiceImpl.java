package ai.traceable.blocking.config.service;

import ai.traceable.blocking.config.service.blockingmodsec.ModsecBlockingManager;
import ai.traceable.blocking.config.service.customsignature.CustomModsecBlockingManager;
import ai.traceable.blocking.config.service.regions.RegionBlockingManager;
import ai.traceable.blocking.config.service.v1.BlockingConfigServiceGrpc.BlockingConfigServiceImplBase;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BlockingConfigServiceImpl extends BlockingConfigServiceImplBase {
  private final RegionBlockingManager regionBlockingManager;
  private final CustomModsecBlockingManager customModsecBlockingManager;
  private final ModsecBlockingManager modsecBlockingManager;

  @Inject
  public BlockingConfigServiceImpl(
      RegionBlockingManager regionBlockingManager,
      CustomModsecBlockingManager customModsecBlockingManager,
      ModsecBlockingManager modsecBlockingManager) {
    this.regionBlockingManager = regionBlockingManager;
    this.customModsecBlockingManager = customModsecBlockingManager;
    this.modsecBlockingManager = modsecBlockingManager;
  }

  @Override
  public void getBlockingRules(
      GetBlockingRulesRequest request, StreamObserver<GetBlockingRulesResponse> responseObserver) {

    GetBlockingRulesResponse.Builder responseBuilder = GetBlockingRulesResponse.newBuilder();

    try {
      responseBuilder.setRegionBlockingRules(
          regionBlockingManager.getEnabledBlockingRules(request.getRegionBlockingRulesHash()));
    } catch (RuntimeException e) {
      log.error("Unable to fetch region blocking rules", e);
    }

    try {
      responseBuilder.setCustomModsecBlockingRules(
          customModsecBlockingManager.getEnabledBlockingRules(
              request.getCustomModsecBlockingRulesHash()));
    } catch (RuntimeException e) {
      log.error("Unable to fetch custom signature blocking rules", e);
    }

    try {
      responseBuilder.setSafeCrsBlockingRules(
          modsecBlockingManager.getBlockingRules(request.getSafeCrsBlockingRulesHash()));
    } catch (RuntimeException e) {
      log.error("Unable to fetch blocking crs blocking rules", e);
    }

    responseObserver.onNext(responseBuilder.build());
    responseObserver.onCompleted();
  }
}
