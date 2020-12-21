package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.DEFAULT_CONFIG;
import static ai.traceable.sensitivedata.config.service.TestUtils.TENANT_ID;
import static ai.traceable.sensitivedata.config.service.TestUtils.getPiiFilterConfigInstance;
import static ai.traceable.sensitivedata.config.service.TestUtils.getPiiFilterConfigValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigResponse;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.UpsertPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.UpsertPiiFilterConfigResponse;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Map;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class PiiFilterConfigServiceImplTest {

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private PiiFilterConfigServiceImpl piiFilterConfigService;
  private MockConfigServiceImpl mockConfigService = new MockConfigServiceImpl();

  @BeforeEach
  void setUp() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(mockConfigService)
            .build()
            .start());
    ManagedChannel managedChannel =
        grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build());
    ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(managedChannel);

    Config config = ConfigFactory.parseMap(Map.of(DEFAULT_CONFIG, Map.of()));
    piiFilterConfigService = new PiiFilterConfigServiceImpl(configServiceBlockingStub, config);
  }

  @Test
  void getPiiFilterConfig() {
    StreamObserver<GetPiiFilterConfigResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            piiFilterConfigService.getPiiFilterConfig(
                GetPiiFilterConfigRequest.getDefaultInstance(), responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    ArgumentCaptor<GetPiiFilterConfigResponse> argumentCaptor =
        ArgumentCaptor.forClass(GetPiiFilterConfigResponse.class);
    verify(responseObserver, times(1)).onNext(argumentCaptor.capture());
    verify(responseObserver, times(1)).onCompleted();
    verify(responseObserver, never()).onError(any(Throwable.class));

    PiiFilterConfig actual = argumentCaptor.getValue().getPiiFilterConfig();
    assertEquals(getPiiFilterConfigInstance(), actual);
  }

  @Test
  void upsertPiiFilterConfig() {
    StreamObserver<UpsertPiiFilterConfigResponse> responseObserver = mock(StreamObserver.class);
    UpsertPiiFilterConfigRequest request =
        UpsertPiiFilterConfigRequest.newBuilder()
            .setPiiFilterConfig(getPiiFilterConfigInstance())
            .build();
    Runnable runnable =
        () -> piiFilterConfigService.upsertPiiFilterConfig(request, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    assertEquals(getPiiFilterConfigValue(), mockConfigService.getUpsertedPiiFilterConfig());
  }
}
