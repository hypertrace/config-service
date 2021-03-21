package ai.traceable.blocking.config.service;

import ai.traceable.blocking.config.service.regions.RegionBlockingManager;
import ai.traceable.blocking.config.service.v1.BlockingConfigServiceGrpc.BlockingConfigServiceImplBase;
import ai.traceable.blocking.config.service.v1.BlockingRule;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesResponse;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
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
    List<BlockingRule> regionBlockingRules;
    try {
      regionBlockingRules = this.regionBlockingManager.getBlockingRules();
    } catch (RuntimeException e) {
      log.error("Unable to get region blocking rules", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription("Unable to get region blocking rules").asException());
      return;
    }

    String currentHash = hashGenerator.generate(regionBlockingRules);
    GetBlockingRulesResponse response =
        currentHash.equals(request.getOmitIfMatchesHash())
            ? GetBlockingRulesResponse.newBuilder().setHash(currentHash).build()
            : GetBlockingRulesResponse.newBuilder()
                .addAllRule(regionBlockingRules)
                .setHash(currentHash)
                .build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
