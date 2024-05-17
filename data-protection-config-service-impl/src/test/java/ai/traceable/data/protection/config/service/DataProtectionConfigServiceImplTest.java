package ai.traceable.data.protection.config.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.data.protection.config.service.v1.DeleteScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.DeleteScopedDataProtectionConfigResponse;
import ai.traceable.data.protection.config.service.v1.GetResolvedScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.GetResolvedScopedDataProtectionConfigResponse;
import ai.traceable.data.protection.config.service.v1.ScopedDataProtectionConfig;
import ai.traceable.data.protection.config.service.v1.UpsertScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.UpsertScopedDataProtectionConfigResponse;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataProtectionConfigServiceImplTest {
  private ConfigManager rulesManager;
  private ConfigValidator rulesValidator;
  private DataProtectionConfigServiceImpl dataProtectionConfigService;
  private final RequestContext requestContext = RequestContext.forTenantId("tenantId");

  @BeforeEach
  void setUp() {
    rulesManager = mock(ConfigManager.class);
    rulesValidator = mock(ConfigValidator.class);
    dataProtectionConfigService = new DataProtectionConfigServiceImpl(rulesValidator, rulesManager);
  }

  @Test
  void testGetResolvedScopedDataProtectionConfig() {
    ScopedDataProtectionConfig dataProtectionConfig =
        ScopedDataProtectionConfig.getDefaultInstance();
    GetResolvedScopedDataProtectionConfigRequest request =
        GetResolvedScopedDataProtectionConfigRequest.getDefaultInstance();
    StreamObserver<GetResolvedScopedDataProtectionConfigResponse> streamObserver =
        mock(StreamObserver.class);

    when(rulesManager.getResolvedScopedDataProtectionConfig(any(), eq(requestContext)))
        .thenReturn(Optional.of(dataProtectionConfig));

    doNothing().when(rulesValidator).validateGetRequest(requestContext, request);

    requestContext.run(
        () ->
            dataProtectionConfigService.getResolvedScopedDataProtectionConfig(
                request, streamObserver));
    verify(streamObserver, times(1))
        .onNext(
            GetResolvedScopedDataProtectionConfigResponse.newBuilder()
                .setConfig(dataProtectionConfig)
                .build());
    verify(streamObserver, times(1)).onCompleted();
  }

  @Test
  void testUpsertScopedDataProtectionConfig() {
    ScopedDataProtectionConfig dataProtectionConfig =
        ScopedDataProtectionConfig.getDefaultInstance();
    UpsertScopedDataProtectionConfigRequest request =
        UpsertScopedDataProtectionConfigRequest.getDefaultInstance();
    StreamObserver<UpsertScopedDataProtectionConfigResponse> streamObserver =
        mock(StreamObserver.class);

    when(rulesManager.upsertScopedDataProtectionConfig(any(), eq(requestContext)))
        .thenReturn(dataProtectionConfig);

    // validation succeeds
    doNothing().when(rulesValidator).validateUpsertRequest(eq(requestContext), eq(request));

    requestContext.run(
        () ->
            dataProtectionConfigService.upsertScopedDataProtectionConfig(request, streamObserver));
    verify(streamObserver, times(1))
        .onNext(
            UpsertScopedDataProtectionConfigResponse.newBuilder()
                .setConfig(dataProtectionConfig)
                .build());
    verify(streamObserver, times(1)).onCompleted();
  }

  @Test
  void testDeleteScopedDataProtectionConfig() {
    DeleteScopedDataProtectionConfigRequest request =
        DeleteScopedDataProtectionConfigRequest.getDefaultInstance();
    StreamObserver<DeleteScopedDataProtectionConfigResponse> streamObserver =
        mock(StreamObserver.class);

    // validation succeeds
    doNothing().when(rulesValidator).validateDeleteRequest(requestContext, request);

    requestContext.run(
        () ->
            dataProtectionConfigService.deleteScopedDataProtectionConfig(request, streamObserver));
    verify(rulesManager, times(1)).deleteScopedDataProtectionConfig(any(), eq(requestContext));
    verify(streamObserver, times(1))
        .onNext(DeleteScopedDataProtectionConfigResponse.getDefaultInstance());
    verify(streamObserver, times(1)).onCompleted();
  }
}
