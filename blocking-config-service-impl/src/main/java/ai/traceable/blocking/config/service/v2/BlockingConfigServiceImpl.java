package ai.traceable.blocking.config.service.v2;

import ai.traceable.blocking.config.service.common.entity.EntityFetcher;
import ai.traceable.blocking.config.service.v2.BlockingConfigServiceGrpc.BlockingConfigServiceImplBase;
import ai.traceable.blocking.config.service.v2.blockingpolicy.BlockingPolicyConfigurationManager;
import ai.traceable.blocking.config.service.v2.iptype.IpTypeBlockingManager;
import ai.traceable.blocking.config.service.v2.regions.RegionBlockingManager;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class BlockingConfigServiceImpl extends BlockingConfigServiceImplBase {
  private final RegionBlockingManager regionBlockingManager;
  private final IpTypeBlockingManager ipTypeBlockingManager;
  private final BlockingPolicyConfigurationManager blockingPolicyConfigurationManager;
  private final EntityFetcher entityFetcher;

  @Inject
  public BlockingConfigServiceImpl(
      RegionBlockingManager regionBlockingManager,
      IpTypeBlockingManager ipTypeBlockingManager,
      BlockingPolicyConfigurationManager blockingPolicyConfigurationManager,
      EntityFetcher entityFetcher) {
    this.regionBlockingManager = regionBlockingManager;
    this.ipTypeBlockingManager = ipTypeBlockingManager;
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

      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException | ExecutionException e) {
      log.error("Get Blocking Rules RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
