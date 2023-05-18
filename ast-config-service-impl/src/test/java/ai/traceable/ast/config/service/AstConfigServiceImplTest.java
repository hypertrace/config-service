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
import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigRequest;
import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigResponse;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesResponse;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesResponse;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigResponse;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesResponse;
import ai.traceable.ast.config.service.v1.IdentifyingAttributes;
import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigResponse;
import ai.traceable.ast.config.service.v1.VulnerabilityMetadataOverrides;
import com.google.protobuf.Duration;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.util.List;
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

  @Nested
  class testEditVulnerabilityMetadataConfig {

    @Test
    @DisplayName("Should update metadata on valid request")
    void should_update_the_plugin_config() {
      VulnerabilityMetadataOverrides vulnerabilityMetadataOverrides =
          VulnerabilityMetadataOverrides.newBuilder()
              .setIdentifyingAttributes(
                  IdentifyingAttributes.newBuilder()
                      .setCategory("category")
                      .setSubcategory("subcategory")
                      .setMetadataId("metadataId")
                      .build())
              .setCvssScore(7.7)
              .build();
      EditVulnerabilityMetadataOverridesRequest request =
          EditVulnerabilityMetadataOverridesRequest.newBuilder()
              .setVulnerabilityMetadataOverrides(vulnerabilityMetadataOverrides)
              .build();

      when(rulesManager.updateVulnerabilityMetadataOverridesConfig(any(), eq(request)))
          .thenReturn(vulnerabilityMetadataOverrides);

      StreamObserver<EditVulnerabilityMetadataOverridesResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () ->
              astConfigService.editVulnerabilityMetadataOverrides(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(
              EditVulnerabilityMetadataOverridesResponse.newBuilder()
                  .setVulnerabilityMetadataOverrides(vulnerabilityMetadataOverrides)
                  .build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("Should throw error on invalid request")
    void should_not_update_the_plugin_config_on_invalid_request() {
      VulnerabilityMetadataOverrides vulnerabilityMetadataOverrides =
          VulnerabilityMetadataOverrides.newBuilder().setCvssScore(7.7).build();
      EditVulnerabilityMetadataOverridesRequest request =
          EditVulnerabilityMetadataOverridesRequest.newBuilder()
              .setVulnerabilityMetadataOverrides(vulnerabilityMetadataOverrides)
              .build();

      doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
          .when(rulesValidator)
          .validateOrThrow(any(), eq(request));

      StreamObserver<EditVulnerabilityMetadataOverridesResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () ->
              astConfigService.editVulnerabilityMetadataOverrides(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }
  }

  @Nested
  class ResetPluginConfigToDefault {
    @Test
    @DisplayName("Should reset plugin config on valid request")
    void should_reset_the_plugin_config_on_valid_request() {
      String metadataId = "metadata_id";
      DeleteVulnerabilityMetadataOverridesConfigRequest request =
          DeleteVulnerabilityMetadataOverridesConfigRequest.newBuilder()
              .setMetadataId(metadataId)
              .build();

      when(rulesManager.deleteVulnerabilityMetadataOverridesConfig(any(), eq(request)))
          .thenReturn(Optional.of(VulnerabilityMetadataOverrides.getDefaultInstance()));

      StreamObserver<DeleteVulnerabilityMetadataOverridesConfigResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () ->
              astConfigService.deleteVulnerabilityMetadataOverridesConfig(
                  request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(DeleteVulnerabilityMetadataOverridesConfigResponse.newBuilder().build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("Should not reset plugin config on invalid request")
    void should_fail_on_invalid_request() {
      DeleteVulnerabilityMetadataOverridesConfigRequest request =
          DeleteVulnerabilityMetadataOverridesConfigRequest.newBuilder().build();

      doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
          .when(rulesValidator)
          .validateOrThrow(any(), eq(request));

      StreamObserver<DeleteVulnerabilityMetadataOverridesConfigResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () ->
              astConfigService.deleteVulnerabilityMetadataOverridesConfig(
                  request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }
  }

  @Nested
  class GetVulnerabilityMetadataOverrides {
    @Test
    @DisplayName("Should get vulnerability metadata on valid request")
    void should_get_vulnerability_metad_overrides_data_on_valid_request() {
      IdentifyingAttributes identifyingAttributes =
          IdentifyingAttributes.newBuilder()
              .setCategory("category")
              .setSubcategory("subcategory")
              .setMetadataId("metadataId")
              .build();
      String metadataId = "metadata_id";
      GetVulnerabilityMetadataOverridesRequest request =
          GetVulnerabilityMetadataOverridesRequest.newBuilder().setMetadataId(metadataId).build();

      when(rulesManager.getVulnerabilityMetadataOverridesConfig(any(), eq(request)))
          .thenReturn(
              Optional.of(
                  VulnerabilityMetadataOverrides.newBuilder()
                      .setIdentifyingAttributes(identifyingAttributes)
                      .build()));

      StreamObserver<GetVulnerabilityMetadataOverridesResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () -> astConfigService.getVulnerabilityMetadataOverrides(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(
              GetVulnerabilityMetadataOverridesResponse.newBuilder()
                  .setVulnerabilityMetadataOverrides(
                      VulnerabilityMetadataOverrides.newBuilder()
                          .setIdentifyingAttributes(identifyingAttributes)
                          .build())
                  .build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("Should fail on invalid request")
    void should_fail_on_invalid_request() {
      GetVulnerabilityMetadataOverridesRequest request =
          GetVulnerabilityMetadataOverridesRequest.newBuilder().build();

      doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
          .when(rulesValidator)
          .validateOrThrow(any(), eq(request));

      StreamObserver<GetVulnerabilityMetadataOverridesResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () -> astConfigService.getVulnerabilityMetadataOverrides(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }
  }

  @Nested
  class GetAllVulnerabilityMetadataOverrides {
    @Test
    @DisplayName("Should get all metadata overrides on valid request")
    void should_get_all_vulnerability_metadata_overrides_data_on_valid_request() {
      IdentifyingAttributes identifyingAttributes =
          IdentifyingAttributes.newBuilder()
              .setCategory("category")
              .setSubcategory("subcategory")
              .setMetadataId("metadataId")
              .build();
      GetAllVulnerabilityMetadataOverridesRequest request =
          GetAllVulnerabilityMetadataOverridesRequest.newBuilder().build();

      when(rulesManager.getAllVulnerabilityMetadataOverridesConfig(any(), eq(request)))
          .thenReturn(
              List.of(
                  VulnerabilityMetadataOverrides.newBuilder()
                      .setIdentifyingAttributes(identifyingAttributes)
                      .build()));

      StreamObserver<GetAllVulnerabilityMetadataOverridesResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () ->
              astConfigService.getAllVulnerabilityMetadataOverrides(
                  request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(
              GetAllVulnerabilityMetadataOverridesResponse.newBuilder()
                  .addAllVulnerabilityMetadataOverrides(
                      List.of(
                          VulnerabilityMetadataOverrides.newBuilder()
                              .setIdentifyingAttributes(identifyingAttributes)
                              .build()))
                  .build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }
  }
}
