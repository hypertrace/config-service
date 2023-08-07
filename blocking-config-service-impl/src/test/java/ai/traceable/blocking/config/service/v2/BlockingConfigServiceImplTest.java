package ai.traceable.blocking.config.service.v2;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.entity.EntityFetcher;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRulesLoader;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplierContext;
import ai.traceable.blocking.config.service.common.rules.fetchers.CustomSignatureRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.MaliciousSourcesRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RegionRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RulesFetcher;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import com.google.protobuf.Duration;
import com.typesafe.config.ConfigFactory;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockingConfigServiceImplTest {
  private static final String TENANT_ID = "tenant1";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final List<BlockingConfigRequestElement> blockingConfigRequestElements =
      List.of(mock(BlockingConfigRequestElement.class), mock(BlockingConfigRequestElement.class));
  private static final List<BlockingConfigResponseElement> sampleResponseElements =
      List.of(
          BlockingConfigResponseElement.newBuilder().setHash("1").build(),
          BlockingConfigResponseElement.newBuilder().setHash("2").build());
  private static final String HASH = "hash";
  private static final String ENVIRONMENT = "env-id";
  private static final BlockingConfigManagerBase blockingManager1 =
      mock(BlockingConfigManagerBase.class);

  private BlockingConfigServiceImpl blockingConfigService;

  @BeforeEach
  void setup() throws ExecutionException {
    BlockingConfigManagerBase blockingManager2 = mock(BlockingConfigManagerBase.class);

    EntityFetcher entityFetcher = mock(EntityFetcher.class);
    doReturn(Optional.of(ENVIRONMENT))
        .when(entityFetcher)
        .getEnvironmentId(REQUEST_CONTEXT, ENVIRONMENT);

    UuidGenerator uuidGenerator = mock(UuidGenerator.class);
    doReturn(HASH).when(uuidGenerator).generateId(anyList());

    doReturn(sampleResponseElements)
        .when(blockingManager1)
        .generateBlockingElements(
            eq(blockingConfigRequestElements),
            argThat(
                blockingRulesSupplier ->
                    blockingRulesSupplier.getRequestContext().equals(REQUEST_CONTEXT)
                        && blockingRulesSupplier.getEnvironmentId().get().equals(ENVIRONMENT)));

    doReturn(sampleResponseElements)
        .when(blockingManager2)
        .generateBlockingElements(
            eq(blockingConfigRequestElements),
            argThat(
                blockingRulesSupplier ->
                    blockingRulesSupplier.getRequestContext().equals(REQUEST_CONTEXT)
                        && blockingRulesSupplier.getEnvironmentId().get().equals(ENVIRONMENT)));

    Map<RulesFetcher.RulesFetcherType, RulesFetcher> rulesFetchers =
        Map.of(
            RulesFetcher.RulesFetcherType.CUSTOM_SIGNATURE,
            mock(CustomSignatureRulesFetcher.class),
            RulesFetcher.RulesFetcherType.REGION,
            mock(RegionRulesFetcher.class),
            RulesFetcher.RulesFetcherType.MALICIOUS_SOURCES,
            mock(MaliciousSourcesRulesFetcher.class));
    IpTypeRulesLoader ipTypeRulesLoader = mock(IpTypeRulesLoader.class);
    when(ipTypeRulesLoader.getLatestDataSupplier())
        .thenReturn(
            () ->
                Arrays.stream(IpLocationType.values())
                    .collect(
                        Collectors.toMap(
                            Function.identity(), ipType -> new IpTypeRuleInfo(ipType))));
    BlockingRulesSupplierContext blockingRulesSupplierContext =
        new BlockingRulesSupplierContext(rulesFetchers, ipTypeRulesLoader);
    this.blockingConfigService =
        new BlockingConfigServiceImpl(
            Set.of(blockingManager1, blockingManager2),
            ConfigFactory.parseMap(Map.of("agent.polling.frequency", "30s")),
            entityFetcher,
            blockingRulesSupplierContext,
            uuidGenerator);
  }

  @Test
  void testValidResponse() {
    StreamObserver<GetBlockingRulesResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setPreviousHash("")
                    .addAllRequestElements(blockingConfigRequestElements)
                    .setEnvironment(ENVIRONMENT)
                    .build(),
                responseObserver);
    REQUEST_CONTEXT.run(runnable);
    verify(responseObserver, times(1))
        .onNext(
            GetBlockingRulesResponse.newBuilder()
                .setHash(HASH)
                .setRefreshAfterDuration(Duration.newBuilder().setSeconds(30))
                .addAllResponseElements(sampleResponseElements)
                .addAllResponseElements(sampleResponseElements)
                .setEnabled(true)
                .build());
  }

  @Test
  void testValidResponseSameHash() {
    StreamObserver<GetBlockingRulesResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setPreviousHash(HASH)
                    .addAllRequestElements(blockingConfigRequestElements)
                    .setEnvironment(ENVIRONMENT)
                    .build(),
                responseObserver);
    REQUEST_CONTEXT.run(runnable);
    verify(responseObserver, times(1))
        .onNext(
            GetBlockingRulesResponse.newBuilder()
                .setHash(HASH)
                .setRefreshAfterDuration(Duration.newBuilder().setSeconds(30))
                .setEnabled(true)
                .build());
  }

  @Test
  void testResponseOnException() {
    doThrow(RuntimeException.class)
        .when(blockingManager1)
        .generateBlockingElements(eq(blockingConfigRequestElements), any());

    StreamObserver<GetBlockingRulesResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            blockingConfigService.getBlockingRules(
                GetBlockingRulesRequest.newBuilder()
                    .setPreviousHash(HASH)
                    .addAllRequestElements(blockingConfigRequestElements)
                    .setEnvironment(ENVIRONMENT)
                    .build(),
                responseObserver);
    REQUEST_CONTEXT.run(runnable);
    verify(responseObserver, times(1)).onError(any(RuntimeException.class));
  }
}
