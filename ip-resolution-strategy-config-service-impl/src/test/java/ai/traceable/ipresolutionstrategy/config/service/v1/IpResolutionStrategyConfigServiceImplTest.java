package ai.traceable.ipresolutionstrategy.config.service.v1;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigServiceGrpc.IpResolutionStrategyConfigServiceImplBase;
import ai.traceable.ipresolutionstrategy.config.service.v1.store.IpResolutionStrategyStore;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class IpResolutionStrategyConfigServiceImplTest {
  private static final String TENANT_ID = "tenant-ip-resolution-test";

  private IpResolutionStrategyStore store;
  private IpResolutionStrategyConfigServiceImplBase service;
  private UuidGenerator uuidGenerator;

  @BeforeEach
  void setup() {
    store = mock(IpResolutionStrategyStore.class);
    uuidGenerator = mock(UuidGenerator.class);
    service = new IpResolutionStrategyConfigServiceImpl(store, uuidGenerator);
  }

  @Test
  @DisplayName("getIpResolutionStrategyConfigs returns configs from store")
  void getConfigs_success() {
    IpResolutionStrategyConfigData data =
        IpResolutionStrategyConfigData.newBuilder()
            .setStrategy(IpResolutionStrategy.newBuilder().setEvaluateAllIpsForBlocking(true))
            .setScope(
                Scope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.getDefaultInstance())
                    .setServiceScope(ServiceScope.getDefaultInstance()))
            .setDisabled(false)
            .build();
    IpResolutionStrategyConfig cfg =
        IpResolutionStrategyConfig.newBuilder().setId("id1").setData(data).build();

    when(store.getAllConfigData(any(), any())).thenReturn(List.of(cfg));

    @SuppressWarnings("unchecked")
    StreamObserver<GetIpResolutionStrategyConfigsResponse> observer = mock(StreamObserver.class);

    Runnable r =
        () ->
            service.getIpResolutionStrategyConfigs(
                GetIpResolutionStrategyConfigsRequest.newBuilder().build(), observer);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, r);

    verify(observer, times(1))
        .onNext(
            GetIpResolutionStrategyConfigsResponse.newBuilder()
                .addAllConfigs(List.of(cfg))
                .build());
    verify(observer, times(1)).onCompleted();
  }

  @Test
  @DisplayName("createIpResolutionStrategyConfig returns created config with generated id")
  void create_success() {
    IpResolutionStrategyConfigData data =
        IpResolutionStrategyConfigData.newBuilder()
            .setStrategy(IpResolutionStrategy.newBuilder().setEvaluateAllIpsForBlocking(true))
            .setScope(Scope.getDefaultInstance())
            .setDisabled(false)
            .build();

    // Exclusion-parity: ID is random on create
    when(uuidGenerator.generateRandomId()).thenReturn("11111111-1111-1111-1111-111111111111");

    when(store.upsertObject(any(), any()))
        .thenAnswer(
            inv -> {
              IpResolutionStrategyConfig argCfg = inv.getArgument(1);
              @SuppressWarnings("unchecked")
              ContextualConfigObject<IpResolutionStrategyConfig> ctx =
                  mock(ContextualConfigObject.class);
              when(ctx.getData()).thenReturn(argCfg);
              return ctx;
            });

    @SuppressWarnings("unchecked")
    StreamObserver<CreateIpResolutionStrategyConfigResponse> observer = mock(StreamObserver.class);

    Runnable r =
        () ->
            service.createIpResolutionStrategyConfig(
                CreateIpResolutionStrategyConfigRequest.newBuilder().setData(data).build(),
                observer);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, r);

    ArgumentCaptor<CreateIpResolutionStrategyConfigResponse> captor =
        ArgumentCaptor.forClass(CreateIpResolutionStrategyConfigResponse.class);
    verify(observer, times(1)).onNext(captor.capture());
    verify(observer, times(1)).onCompleted();

    CreateIpResolutionStrategyConfigResponse resp = captor.getValue();
    IpResolutionStrategyConfig returnedCfg = resp.getConfig();
    // Data should match
    org.junit.jupiter.api.Assertions.assertEquals(data, returnedCfg.getData());
    // Id is random-based UUID returned by mocked UuidGenerator
    org.junit.jupiter.api.Assertions.assertEquals(
        "11111111-1111-1111-1111-111111111111", returnedCfg.getId());
  }

  @Test
  @DisplayName("updateIpResolutionStrategyConfig returns NOT_FOUND when missing")
  void update_notFound() {
    when(store.getData(any(), eq("missing"))).thenReturn(Optional.empty());

    @SuppressWarnings("unchecked")
    StreamObserver<UpdateIpResolutionStrategyConfigResponse> observer = mock(StreamObserver.class);

    Runnable r =
        () ->
            service.updateIpResolutionStrategyConfig(
                UpdateIpResolutionStrategyConfigRequest.newBuilder()
                    .setId("missing")
                    .setData(IpResolutionStrategyConfigData.getDefaultInstance())
                    .build(),
                observer);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, r);

    verify(observer, times(1))
        .onError(argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.NOT_FOUND));
  }

  @Test
  @DisplayName("deleteIpResolutionStrategyConfig succeeds when present")
  void delete_success() {
    @SuppressWarnings("unchecked")
    DeletedContextualConfigObject<IpResolutionStrategyConfig> deleted =
        (DeletedContextualConfigObject<IpResolutionStrategyConfig>)
            mock(DeletedContextualConfigObject.class);
    when(store.deleteObject(any(), eq("id"))).thenReturn(Optional.of(deleted));

    @SuppressWarnings("unchecked")
    StreamObserver<DeleteIpResolutionStrategyConfigResponse> observer = mock(StreamObserver.class);

    Runnable r =
        () ->
            service.deleteIpResolutionStrategyConfig(
                DeleteIpResolutionStrategyConfigRequest.newBuilder().setId("id").build(), observer);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, r);

    verify(observer, times(1))
        .onNext(DeleteIpResolutionStrategyConfigResponse.getDefaultInstance());
    verify(observer, times(1)).onCompleted();
  }

  @Test
  @DisplayName("deleteIpResolutionStrategyConfig returns NOT_FOUND when absent")
  void delete_notFound() {
    when(store.deleteObject(any(), eq("id"))).thenReturn(Optional.empty());

    @SuppressWarnings("unchecked")
    StreamObserver<DeleteIpResolutionStrategyConfigResponse> observer = mock(StreamObserver.class);

    Runnable r =
        () ->
            service.deleteIpResolutionStrategyConfig(
                DeleteIpResolutionStrategyConfigRequest.newBuilder().setId("id").build(), observer);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, r);

    verify(observer, times(1))
        .onError(argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.NOT_FOUND));
  }

  @Test
  @DisplayName("createIpResolutionStrategyConfig propagates exceptions to observer")
  void create_exception() {
    when(store.upsertObject(any(), any())).thenThrow(new RuntimeException("boom"));

    @SuppressWarnings("unchecked")
    StreamObserver<CreateIpResolutionStrategyConfigResponse> observer = mock(StreamObserver.class);

    Runnable r =
        () ->
            service.createIpResolutionStrategyConfig(
                CreateIpResolutionStrategyConfigRequest.newBuilder()
                    .setData(IpResolutionStrategyConfigData.getDefaultInstance())
                    .build(),
                observer);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, r);

    verify(observer, times(1)).onError(argThat(err -> err instanceof RuntimeException));
  }

  @Test
  @DisplayName("updateIpResolutionStrategyConfig updates when present")
  void update_success() {
    IpResolutionStrategyConfigData data =
        IpResolutionStrategyConfigData.newBuilder()
            .setStrategy(IpResolutionStrategy.newBuilder().setEvaluateAllIpsForBlocking(true))
            .setScope(Scope.getDefaultInstance())
            .build();
    when(store.getData(any(), eq("id")))
        .thenReturn(Optional.of(IpResolutionStrategyConfig.newBuilder().setId("id").build()));
    when(store.upsertObject(any(), any()))
        .thenAnswer(
            inv -> {
              IpResolutionStrategyConfig argCfg = inv.getArgument(1);
              @SuppressWarnings("unchecked")
              ContextualConfigObject<IpResolutionStrategyConfig> ctx =
                  mock(ContextualConfigObject.class);
              when(ctx.getData()).thenReturn(argCfg);
              return ctx;
            });

    @SuppressWarnings("unchecked")
    StreamObserver<UpdateIpResolutionStrategyConfigResponse> observer = mock(StreamObserver.class);

    Runnable r =
        () ->
            service.updateIpResolutionStrategyConfig(
                UpdateIpResolutionStrategyConfigRequest.newBuilder()
                    .setId("id")
                    .setData(data)
                    .build(),
                observer);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, r);

    verify(observer, times(1))
        .onNext(
            argThat(
                resp ->
                    resp.getConfig().getId().equals("id")
                        && resp.getConfig().getData().equals(data)));
    verify(observer, times(1)).onCompleted();
  }

  @Test
  @DisplayName("getIpResolutionStrategyConfigs propagates exceptions to observer")
  void get_exception() {
    when(store.getAllConfigData(any(), any())).thenThrow(new RuntimeException("boom"));

    @SuppressWarnings("unchecked")
    StreamObserver<GetIpResolutionStrategyConfigsResponse> observer = mock(StreamObserver.class);

    Runnable r =
        () ->
            service.getIpResolutionStrategyConfigs(
                GetIpResolutionStrategyConfigsRequest.newBuilder().build(), observer);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, r);

    verify(observer, times(1)).onError(argThat(err -> err instanceof RuntimeException));
  }
}
