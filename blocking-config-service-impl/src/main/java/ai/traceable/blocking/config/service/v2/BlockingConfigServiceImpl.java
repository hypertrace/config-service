package ai.traceable.blocking.config.service.v2;

import ai.traceable.blocking.config.service.common.entity.EntityFetcher;
import ai.traceable.blocking.config.service.v2.BlockingConfigServiceGrpc.BlockingConfigServiceImplBase;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import com.google.protobuf.Duration;
import com.typesafe.config.Config;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class BlockingConfigServiceImpl extends BlockingConfigServiceImplBase {
  private static final String AGENT_POLLING_FREQUENCY_CONFIG_NAME = "agent.polling.frequency";
  private final Set<BlockingConfigManagerBase> blockingConfigManagers;
  private final java.time.Duration agentPollingFrequency;
  private final EntityFetcher entityFetcher;
  private final UuidGenerator uuidGenerator;
  private final FeatureCachingClient featureCachingClient;

  @Inject
  public BlockingConfigServiceImpl(
      Set<BlockingConfigManagerBase> blockingConfigManagers,
      Config config,
      EntityFetcher entityFetcher,
      UuidGenerator uuidGenerator,
      FeatureCachingClient featureCachingClient) {
    this.blockingConfigManagers = blockingConfigManagers;
    this.agentPollingFrequency = config.getDuration(AGENT_POLLING_FREQUENCY_CONFIG_NAME);
    this.entityFetcher = entityFetcher;
    this.uuidGenerator = uuidGenerator;
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  public void getBlockingRules(
      GetBlockingRulesRequest request, StreamObserver<GetBlockingRulesResponse> responseObserver) {

    GetBlockingRulesResponse.Builder responseBuilder = GetBlockingRulesResponse.newBuilder();
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();

      if (featureCachingClient.isBlockingConfigV2Enabled(requestContext)) {
        Optional<String> environmentId =
            entityFetcher.getEnvironmentId(requestContext, request.getEnvironment());

        List<BlockingConfigResponseElement> responseElements =
            blockingConfigManagers.stream()
                .flatMap(
                    blockingConfigManagerBase ->
                        blockingConfigManagerBase
                            .generateBlockingElements(
                                request.getRequestElementsList(), requestContext, environmentId)
                            .stream())
                .collect(Collectors.toUnmodifiableList());

        String hash =
            uuidGenerator.generateId(
                responseElements.stream()
                    .map(BlockingConfigResponseElement::getHash)
                    .collect(Collectors.toUnmodifiableList()));

        responseBuilder.setHash(hash);
        if (!request.getPreviousHash().equals(hash)) {
          responseBuilder.addAllResponseElements(responseElements);
        }

        responseBuilder.setRefreshAfterDuration(
            Duration.newBuilder()
                .setSeconds(agentPollingFrequency.getSeconds())
                .setNanos(agentPollingFrequency.getNano())
                .build());

        responseBuilder.setEnabled(true);
      } else {
        log.warn("Blocking config v2 not enabled for request context: {}", requestContext);
        responseBuilder.setEnabled(false);
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException | ExecutionException e) {
      log.error("Get Blocking Rules RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
