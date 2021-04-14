package ai.traceable.blocking.config.service;

import ai.traceable.blocking.config.service.regions.RegionBlockingManager;
import ai.traceable.blocking.config.service.v1.BlockingConfigServiceGrpc.BlockingConfigServiceImplBase;
import ai.traceable.blocking.config.service.v1.BlockingRules;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesResponse;
import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BlockingConfigServiceImpl extends BlockingConfigServiceImplBase {
  private final HashGenerator hashGenerator;
  private final RegionBlockingManager regionBlockingManager;

  @Inject
  BlockingConfigServiceImpl(
      HashGenerator hashGenerator, RegionBlockingManager regionBlockingManager) {
    this.hashGenerator = hashGenerator;
    this.regionBlockingManager = regionBlockingManager;
  }

  @Override
  public void getBlockingRules(
      GetBlockingRulesRequest request, StreamObserver<GetBlockingRulesResponse> responseObserver) {
    RegionBlockingRules regionBlockingRules;
    try {
      regionBlockingRules = this.regionBlockingManager.getBlockingRules();
    } catch (RuntimeException e) {
      log.error("Unable to get region blocking rules", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription("Unable to get region blocking rules").asException());
      return;
    }

    BlockingRules blockingRules =
        BlockingRules.newBuilder().setRegionBlockingRules(regionBlockingRules).build();
    String currentHash = hashGenerator.generate(blockingRules);

    GetBlockingRulesResponse.Builder responseBuilder =
        GetBlockingRulesResponse.newBuilder().setHash(currentHash);
    if (!currentHash.equals(request.getOmitIfMatchesHash())) {
      responseBuilder.setBlockingRules(blockingRules);
    }

    responseObserver.onNext(responseBuilder.build());
    responseObserver.onCompleted();
  }
}
