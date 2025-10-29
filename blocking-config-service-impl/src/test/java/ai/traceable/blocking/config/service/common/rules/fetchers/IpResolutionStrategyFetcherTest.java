package ai.traceable.blocking.config.service.common.rules.fetchers;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.blocking.config.service.v2.IpResolutionStrategy;
import ai.traceable.ipresolutionstrategy.config.service.v1.EnvironmentScope;
import ai.traceable.ipresolutionstrategy.config.service.v1.GetIpResolutionStrategyConfigsRequest;
import ai.traceable.ipresolutionstrategy.config.service.v1.GetIpResolutionStrategyConfigsResponse;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfig;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigData;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigServiceGrpc;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigServiceGrpc.IpResolutionStrategyConfigServiceBlockingStub;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigServiceGrpc.IpResolutionStrategyConfigServiceImplBase;
import ai.traceable.ipresolutionstrategy.config.service.v1.Scope;
import ai.traceable.ipresolutionstrategy.config.service.v1.ServiceScope;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class IpResolutionStrategyFetcherTest {
  private Server server;
  private ManagedChannel channel;
  private IpResolutionStrategyConfigServiceBlockingStub stub;

  @BeforeEach
  void setup() throws Exception {
    String name = InProcessServerBuilder.generateName();
    server =
        InProcessServerBuilder.forName(name)
            .directExecutor()
            .addService(new FakeService())
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    stub = IpResolutionStrategyConfigServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void tearDown() {
    if (channel != null) channel.shutdownNow();
    if (server != null) server.shutdownNow();
  }

  @Test
  @DisplayName("returns empty when service names empty or null")
  void emptyInputs() {
    IpResolutionStrategyFetcher fetcher =
        new IpResolutionStrategyFetcher(stub, ClientConfig.DEFAULT);
    Map<String, IpResolutionStrategy> res1 =
        fetcher.fetchStrategies(RequestContext.forTenantId("t"), Optional.of("prod"), Set.of());
    assertTrue(res1.isEmpty());
    Map<String, IpResolutionStrategy> res2 =
        fetcher.fetchStrategies(RequestContext.forTenantId("t"), Optional.of("prod"), null);
    assertTrue(res2.isEmpty());
  }

  @Test
  @DisplayName("returns only service-scoped strategies; no env-wide fallback")
  void selectionServiceScopedOnly() {
    IpResolutionStrategyFetcher fetcher =
        new IpResolutionStrategyFetcher(stub, new ClientConfig(Duration.ofSeconds(2)));

    Map<String, IpResolutionStrategy> result =
        fetcher.fetchStrategies(
            RequestContext.forTenantId("tenant"), Optional.of("prod"), Set.of("svc1", "svc2"));

    // Only service-specific entries are returned
    assertEquals(1, result.size());
    // svc1 should pick service-specific
    IpResolutionStrategy svc1Strategy = result.get("svc1");
    assertNotNull(svc1Strategy);
    assertEquals("svc1-src", svc1Strategy.getSources(0).getSourceAttributeName());
    // svc2 has no service-scoped config; no env-wide fallback expected
    assertFalse(result.containsKey("svc2"));
  }

  private static class FakeService extends IpResolutionStrategyConfigServiceImplBase {
    @Override
    public void getIpResolutionStrategyConfigs(
        GetIpResolutionStrategyConfigsRequest request,
        StreamObserver<GetIpResolutionStrategyConfigsResponse> responseObserver) {
      // Build configs: service-specific, env-wide, global, and a default strategy to be ignored
      IpResolutionStrategyConfig svcSpecific =
          buildCfg(
              "id-svc",
              false,
              List.of("prod"),
              List.of("svc1"),
              ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategy.newBuilder()
                  .addSources(
                      ai.traceable.ipresolutionstrategy.config.service.v1.IpSource.newBuilder()
                          .setSourceAttributeName("svc1-src"))
                  .build());

      IpResolutionStrategyConfig envWide =
          buildCfg(
              "id-env",
              false,
              List.of("prod"),
              List.of(),
              ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategy.newBuilder()
                  .addSources(
                      ai.traceable.ipresolutionstrategy.config.service.v1.IpSource.newBuilder()
                          .setSourceAttributeName("env-src"))
                  .build());

      IpResolutionStrategyConfig global =
          buildCfg(
              "id-global",
              false,
              List.of(),
              List.of(),
              ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategy.newBuilder()
                  .addSources(
                      ai.traceable.ipresolutionstrategy.config.service.v1.IpSource.newBuilder()
                          .setSourceAttributeName("global-src"))
                  .build());

      IpResolutionStrategyConfig defaultStrategyCfg =
          buildCfg(
              "id-default",
              false,
              List.of("prod"),
              List.of("svc1"),
              ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategy
                  .getDefaultInstance());

      GetIpResolutionStrategyConfigsResponse resp =
          GetIpResolutionStrategyConfigsResponse.newBuilder()
              .addConfigs(svcSpecific)
              .addConfigs(envWide)
              .addConfigs(global)
              .addConfigs(defaultStrategyCfg)
              .build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();
    }

    private static IpResolutionStrategyConfig buildCfg(
        String id,
        boolean disabled,
        List<String> envNames,
        List<String> serviceNames,
        ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategy strategy) {
      IpResolutionStrategyConfigData data =
          IpResolutionStrategyConfigData.newBuilder()
              .setStrategy(strategy)
              .setDisabled(disabled)
              .setScope(
                  Scope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder().addAllEnvironmentNames(envNames))
                      .setServiceScope(ServiceScope.newBuilder().addAllServiceNames(serviceNames)))
              .build();
      return IpResolutionStrategyConfig.newBuilder().setId(id).setData(data).build();
    }
  }
}
