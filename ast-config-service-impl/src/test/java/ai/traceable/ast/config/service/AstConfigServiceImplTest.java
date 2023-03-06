package ai.traceable.ast.config.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.ast.config.service.rules.RulesManager;
import ai.traceable.ast.config.service.rules.RulesValidator;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigResponse;
import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigResponse;
import com.google.protobuf.Duration;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class AstConfigServiceImplTest {
  private static final String TENANT_ID = "default-tenant";
  private RulesValidator rulesValidator;
  private RulesManager rulesManager;
  private AstConfigServiceConfig config;
  private AstConfigServiceImpl astConfigService;

  @BeforeEach
  void setup() {
    rulesValidator = mock(RulesValidator.class);
    rulesManager = mock(RulesManager.class);
    config = mock(AstConfigServiceConfig.class);
    astConfigService = new AstConfigServiceImpl(rulesValidator, rulesManager, config);
  }

  @Nested
  class testUpdateScanPurgeConfig {
    @Test
    @DisplayName("Should fail on invalid request")
    void should_fail_invalid_request() {
      UpdateScanPurgeConfigRequest request = UpdateScanPurgeConfigRequest.getDefaultInstance();

      doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
          .when(rulesValidator)
          .validateOrThrow(any(), eq(request));

      StreamObserver<UpdateScanPurgeConfigResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () -> astConfigService.updateScanPurgeConfig(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("Should fail when unable to update config")
    void should_fail_unable_to_update() {
      UpdateScanPurgeConfigRequest request = UpdateScanPurgeConfigRequest.getDefaultInstance();

      when(rulesManager.updateScanPurgeConfig(any(), eq(request)))
          .thenThrow(Status.INTERNAL.asRuntimeException());

      StreamObserver<UpdateScanPurgeConfigResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () -> astConfigService.updateScanPurgeConfig(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.INTERNAL));
    }

    @Test
    @DisplayName("Should update config on valid request")
    void should_update_valid_request() {
      ScanPurgeConfig purgeConfig =
          ScanPurgeConfig.newBuilder()
              .setPurgeDuration(Duration.newBuilder().setSeconds(1234))
              .build();
      UpdateScanPurgeConfigRequest request =
          UpdateScanPurgeConfigRequest.newBuilder().setPurgeConfig(purgeConfig).build();

      when(rulesManager.updateScanPurgeConfig(any(), eq(request))).thenReturn(purgeConfig);

      StreamObserver<UpdateScanPurgeConfigResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () -> astConfigService.updateScanPurgeConfig(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(UpdateScanPurgeConfigResponse.newBuilder().setPurgeConfig(purgeConfig).build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }
  }

  @Nested
  class testGetScanPurgeConfig {
    @Test
    @DisplayName("Should fail on invalid request")
    void should_fail_invalid_request() {
      GetScanPurgeConfigRequest request = GetScanPurgeConfigRequest.getDefaultInstance();

      doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
          .when(rulesValidator)
          .validateOrThrow(any(), eq(request));

      StreamObserver<GetScanPurgeConfigResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () -> astConfigService.getScanPurgeConfig(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("Should fail when unable to fetch")
    void should_fail_unable_to_fetch() {
      GetScanPurgeConfigRequest request = GetScanPurgeConfigRequest.getDefaultInstance();

      when(rulesManager.getScanPurgeConfig(any())).thenThrow(Status.INTERNAL.asRuntimeException());

      StreamObserver<GetScanPurgeConfigResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () -> astConfigService.getScanPurgeConfig(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.INTERNAL));
    }

    @Test
    @DisplayName("Should get config on valid request")
    void should_get_config_valid_request() {
      ScanPurgeConfig purgeConfig =
          ScanPurgeConfig.newBuilder()
              .setPurgeDuration(Duration.newBuilder().setSeconds(1234))
              .build();
      GetScanPurgeConfigRequest request = GetScanPurgeConfigRequest.getDefaultInstance();

      when(rulesManager.getScanPurgeConfig(any())).thenReturn(Optional.of(purgeConfig));

      StreamObserver<GetScanPurgeConfigResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () -> astConfigService.getScanPurgeConfig(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(GetScanPurgeConfigResponse.newBuilder().setPurgeConfig(purgeConfig).build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("Should get config from default value on valid request")
    void should_get_config_from_default_valid_request() {
      Duration duration = Duration.newBuilder().setSeconds(1234).build();
      ScanPurgeConfig purgeConfig = ScanPurgeConfig.newBuilder().setPurgeDuration(duration).build();
      GetScanPurgeConfigRequest request = GetScanPurgeConfigRequest.getDefaultInstance();

      when(rulesManager.getScanPurgeConfig(any())).thenReturn(Optional.empty());
      when(config.getDefaultPurgeDuration()).thenReturn(duration);

      StreamObserver<GetScanPurgeConfigResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () -> astConfigService.getScanPurgeConfig(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(GetScanPurgeConfigResponse.newBuilder().setPurgeConfig(purgeConfig).build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }
  }
}
