package ai.traceable.ast.config.service;

import static ai.traceable.ast.config.service.v1.ApiType.API_TYPE_HTTP;
import static ai.traceable.ast.config.service.v1.ApiType.API_TYPE_UNSPECIFIED;
import static ai.traceable.ast.config.service.v1.ApiType.UNRECOGNIZED;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.ast.config.service.configs.AstConfigServiceConfig;
import ai.traceable.ast.config.service.manager.AstOverridesManager;
import ai.traceable.ast.config.service.rules.CustomTestPluginManager;
import ai.traceable.ast.config.service.rules.RulesManager;
import ai.traceable.ast.config.service.store.AstOverridesStore;
import ai.traceable.ast.config.service.store.CustomTestPluginStore;
import ai.traceable.ast.config.service.v1.ApiType;
import ai.traceable.ast.config.service.v1.AstConfigServiceGrpc;
import ai.traceable.ast.config.service.v1.AstOverride;
import ai.traceable.ast.config.service.v1.AstOverrideFilter;
import ai.traceable.ast.config.service.v1.AstOverrideInfo;
import ai.traceable.ast.config.service.v1.CodeSnippetDetails;
import ai.traceable.ast.config.service.v1.CodeSnippetType;
import ai.traceable.ast.config.service.v1.CreateAstOverrideRequest;
import ai.traceable.ast.config.service.v1.CreateCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.CreateCustomTestPluginResponse;
import ai.traceable.ast.config.service.v1.CustomTestPlugin;
import ai.traceable.ast.config.service.v1.CustomTestPluginFilter;
import ai.traceable.ast.config.service.v1.DeleteAstOverridesRequest;
import ai.traceable.ast.config.service.v1.DeleteCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.DeleteCustomTestPluginResponse;
import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigRequest;
import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigResponse;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesResponse;
import ai.traceable.ast.config.service.v1.EnvironmentScope;
import ai.traceable.ast.config.service.v1.GetAllCustomTestPluginsRequest;
import ai.traceable.ast.config.service.v1.GetAllCustomTestPluginsResponse;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesResponse;
import ai.traceable.ast.config.service.v1.GetAstOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAstOverridesResponse;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigResponse;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesResponse;
import ai.traceable.ast.config.service.v1.IdFilter;
import ai.traceable.ast.config.service.v1.IdentifyingAttributes;
import ai.traceable.ast.config.service.v1.MutationOverride;
import ai.traceable.ast.config.service.v1.OverrideConfig;
import ai.traceable.ast.config.service.v1.OverrideScope;
import ai.traceable.ast.config.service.v1.PluginScope;
import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.StringList;
import ai.traceable.ast.config.service.v1.SystemDefinedMutationOverride;
import ai.traceable.ast.config.service.v1.UpdateAstOverrideRequest;
import ai.traceable.ast.config.service.v1.UpdateCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.UpdateCustomTestPluginResponse;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigResponse;
import ai.traceable.ast.config.service.v1.VulnerabilityMetadataOverrides;
import ai.traceable.ast.config.service.validation.AstConfigServiceRequestValidator;
import ai.traceable.config.utils.TimestampConverter;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class AstConfigServiceImplTest {
  private static final String TENANT_ID = "default-tenant";
  private AstConfigServiceRequestValidator requestValidator;
  private RulesManager rulesManager;
  private AstConfigServiceConfig config;
  private AstConfigServiceImpl astConfigService;
  private CustomTestPluginStore customTestPluginStore;
  private TimestampConverter timestampConverter;
  private MockGenericConfigService mockGenericConfigService;
  private AstConfigServiceGrpc.AstConfigServiceBlockingStub astConfigServiceBlockingStub;
  List<ApiType> DEFAULT_SUPPORTED_API_TYPES =
      Arrays.stream(ApiType.values())
          .filter(apiType -> apiType != API_TYPE_UNSPECIFIED && apiType != UNRECOGNIZED)
          .collect(toUnmodifiableList());

  @BeforeEach
  void setup() {
    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDeleteAll();

    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());

    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);

    this.astConfigServiceBlockingStub =
        AstConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());

    requestValidator = mock(AstConfigServiceRequestValidator.class);
    rulesManager = mock(RulesManager.class);
    config = mock(AstConfigServiceConfig.class);
    customTestPluginStore = mock(CustomTestPluginStore.class);
    CustomTestPluginManager customTestPluginManager =
        new CustomTestPluginManager(customTestPluginStore);
    timestampConverter = mock(TimestampConverter.class);
    this.astConfigService =
        new AstConfigServiceImpl(
            requestValidator,
            rulesManager,
            config,
            customTestPluginManager,
            new AstOverridesManager(
                new AstOverridesStore(genericStub, configChangeEventGenerator),
                timestampConverter));
    this.mockGenericConfigService.addService(this.astConfigService).start();
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Nested
  class testUpdateScanPurgeConfig {
    @Test
    @DisplayName("Should fail on invalid request")
    void should_fail_invalid_request() {
      UpdateScanPurgeConfigRequest request = UpdateScanPurgeConfigRequest.getDefaultInstance();

      doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
          .when(requestValidator)
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
          .when(requestValidator)
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
      ScanPurgeConfig purgeConfig =
          ScanPurgeConfig.newBuilder()
              .setPurgeDuration(duration)
              .setScanRetentionLimitPerSuite(10)
              .build();
      GetScanPurgeConfigRequest request = GetScanPurgeConfigRequest.getDefaultInstance();

      when(rulesManager.getScanPurgeConfig(any())).thenReturn(Optional.empty());
      when(config.getDefaultPurgeDuration()).thenReturn(duration);
      when(config.getDefaultScanRetentionLimitPerSuite()).thenReturn(10);
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
          .when(requestValidator)
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
          .when(requestValidator)
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
          .when(requestValidator)
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

  @Nested
  class GetAllCustomTestPlugin {
    @Test
    @DisplayName("Should get all custom test plugin without filter on valid request")
    void should_get_all_custom_test_plugin_without_filter() {
      GetAllCustomTestPluginsRequest request = GetAllCustomTestPluginsRequest.newBuilder().build();
      when(customTestPluginStore.getAllConfigData(any()))
          .thenReturn(
              List.of(
                  CustomTestPlugin.newBuilder()
                      .setName("custom-test")
                      .setCodeSnippetDetails(
                          CodeSnippetDetails.newBuilder()
                              .setCodeSnippet("code-snippet")
                              .setCodeSnippetType(
                                  CodeSnippetType
                                      .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                      .build()));

      StreamObserver<GetAllCustomTestPluginsResponse> responseStreamObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () -> astConfigService.getAllCustomTestPlugins(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(
              GetAllCustomTestPluginsResponse.newBuilder()
                  .addAllCustomTestPlugins(
                      List.of(
                          CustomTestPlugin.newBuilder()
                              .setName("custom-test")
                              .setCodeSnippetDetails(
                                  CodeSnippetDetails.newBuilder()
                                      .setCodeSnippet("code-snippet")
                                      .setCodeSnippetType(
                                          CodeSnippetType
                                              .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                              .addAllSupportedApiTypes(DEFAULT_SUPPORTED_API_TYPES)
                              .build()))
                  .build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("Should get all custom test plugin with filter on valid request")
    void should_get_all_custom_test_plugin_with_filter() {
      GetAllCustomTestPluginsRequest request =
          GetAllCustomTestPluginsRequest.newBuilder()
              .addFilters(
                  CustomTestPluginFilter.newBuilder()
                      .setIdFilter(StringList.newBuilder().addAllValues(List.of("id1"))))
              .build();

      when(customTestPluginStore.getAllConfigData(any()))
          .thenReturn(
              List.of(
                  CustomTestPlugin.newBuilder()
                      .setName("custom-test")
                      .setId("id1")
                      .setCodeSnippetDetails(
                          CodeSnippetDetails.newBuilder()
                              .setCodeSnippet("code-snippet")
                              .setCodeSnippetType(
                                  CodeSnippetType
                                      .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                      .build(),
                  CustomTestPlugin.newBuilder()
                      .setName("custom-test")
                      .setId("id2")
                      .setCodeSnippetDetails(
                          CodeSnippetDetails.newBuilder()
                              .setCodeSnippet("code-snippet")
                              .setCodeSnippetType(
                                  CodeSnippetType
                                      .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                      .build()));

      StreamObserver<GetAllCustomTestPluginsResponse> responseStreamObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () -> astConfigService.getAllCustomTestPlugins(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(
              GetAllCustomTestPluginsResponse.newBuilder()
                  .addAllCustomTestPlugins(
                      List.of(
                          CustomTestPlugin.newBuilder()
                              .setId("id1")
                              .setName("custom-test")
                              .setCodeSnippetDetails(
                                  CodeSnippetDetails.newBuilder()
                                      .setCodeSnippet("code-snippet")
                                      .setCodeSnippetType(
                                          CodeSnippetType
                                              .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                              .addAllSupportedApiTypes(DEFAULT_SUPPORTED_API_TYPES)
                              .build()))
                  .build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("Should get all custom test plugin with environment filter on valid request")
    void should_get_all_custom_test_plugin_with_environment_filter() {
      GetAllCustomTestPluginsRequest request =
          GetAllCustomTestPluginsRequest.newBuilder()
              .addFilters(
                  CustomTestPluginFilter.newBuilder()
                      .setEnvIdFilter(StringList.newBuilder().addAllValues(List.of("env1"))))
              .build();

      when(customTestPluginStore.getAllConfigData(any()))
          .thenReturn(
              List.of(
                  CustomTestPlugin.newBuilder()
                      .setName("custom-test-env1")
                      .setId("id1")
                      .setPluginScope(
                          PluginScope.newBuilder()
                              .setEnvironmentScope(
                                  EnvironmentScope.newBuilder()
                                      .addAllEnvironmentIds(List.of("env1", "env2"))))
                      .setCodeSnippetDetails(
                          CodeSnippetDetails.newBuilder()
                              .setCodeSnippet("code-snippet")
                              .setCodeSnippetType(
                                  CodeSnippetType
                                      .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                      .build(),
                  CustomTestPlugin.newBuilder()
                      .setName("custom-test-env3")
                      .setId("id2")
                      .setPluginScope(
                          PluginScope.newBuilder()
                              .setEnvironmentScope(
                                  EnvironmentScope.newBuilder()
                                      .addAllEnvironmentIds(List.of("env3", "env4"))))
                      .setCodeSnippetDetails(
                          CodeSnippetDetails.newBuilder()
                              .setCodeSnippet("code-snippet")
                              .setCodeSnippetType(
                                  CodeSnippetType
                                      .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                      .build(),
                  CustomTestPlugin.newBuilder()
                      .setName("custom-test-no-scope")
                      .setId("id3")
                      .setCodeSnippetDetails(
                          CodeSnippetDetails.newBuilder()
                              .setCodeSnippet("code-snippet")
                              .setCodeSnippetType(
                                  CodeSnippetType
                                      .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                      .build()));

      StreamObserver<GetAllCustomTestPluginsResponse> responseStreamObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () -> astConfigService.getAllCustomTestPlugins(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(
              GetAllCustomTestPluginsResponse.newBuilder()
                  .addAllCustomTestPlugins(
                      List.of(
                          CustomTestPlugin.newBuilder()
                              .setId("id1")
                              .setName("custom-test-env1")
                              .setPluginScope(
                                  PluginScope.newBuilder()
                                      .setEnvironmentScope(
                                          EnvironmentScope.newBuilder()
                                              .addAllEnvironmentIds(List.of("env1", "env2"))))
                              .setCodeSnippetDetails(
                                  CodeSnippetDetails.newBuilder()
                                      .setCodeSnippet("code-snippet")
                                      .setCodeSnippetType(
                                          CodeSnippetType
                                              .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                              .addAllSupportedApiTypes(DEFAULT_SUPPORTED_API_TYPES)
                              .build(),
                          CustomTestPlugin.newBuilder()
                              .setId("id3")
                              .setName("custom-test-no-scope")
                              .setCodeSnippetDetails(
                                  CodeSnippetDetails.newBuilder()
                                      .setCodeSnippet("code-snippet")
                                      .setCodeSnippetType(
                                          CodeSnippetType
                                              .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                              .addAllSupportedApiTypes(DEFAULT_SUPPORTED_API_TYPES)
                              .build()))
                  .build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("Should get all custom test plugin with multiple filters")
    void should_get_all_custom_test_plugin_with_multiple_filters() {
      GetAllCustomTestPluginsRequest request =
          GetAllCustomTestPluginsRequest.newBuilder()
              .addFilters(
                  CustomTestPluginFilter.newBuilder()
                      .setIdFilter(StringList.newBuilder().addAllValues(List.of("id1", "id2"))))
              .addFilters(
                  CustomTestPluginFilter.newBuilder()
                      .setEnvIdFilter(StringList.newBuilder().addAllValues(List.of("env1"))))
              .build();

      when(customTestPluginStore.getAllConfigData(any()))
          .thenReturn(
              List.of(
                  CustomTestPlugin.newBuilder()
                      .setName("custom-test-matching")
                      .setId("id1")
                      .setPluginScope(
                          PluginScope.newBuilder()
                              .setEnvironmentScope(
                                  EnvironmentScope.newBuilder()
                                      .addAllEnvironmentIds(List.of("env1"))))
                      .setCodeSnippetDetails(
                          CodeSnippetDetails.newBuilder()
                              .setCodeSnippet("code-snippet")
                              .setCodeSnippetType(
                                  CodeSnippetType
                                      .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                      .build(),
                  CustomTestPlugin.newBuilder()
                      .setName("custom-test-wrong-env")
                      .setId("id2")
                      .setPluginScope(
                          PluginScope.newBuilder()
                              .setEnvironmentScope(
                                  EnvironmentScope.newBuilder()
                                      .addAllEnvironmentIds(List.of("env2"))))
                      .setCodeSnippetDetails(
                          CodeSnippetDetails.newBuilder()
                              .setCodeSnippet("code-snippet")
                              .setCodeSnippetType(
                                  CodeSnippetType
                                      .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                      .build(),
                  CustomTestPlugin.newBuilder()
                      .setName("custom-test-wrong-id")
                      .setId("id3")
                      .setPluginScope(
                          PluginScope.newBuilder()
                              .setEnvironmentScope(
                                  EnvironmentScope.newBuilder()
                                      .addAllEnvironmentIds(List.of("env1"))))
                      .setCodeSnippetDetails(
                          CodeSnippetDetails.newBuilder()
                              .setCodeSnippet("code-snippet")
                              .setCodeSnippetType(
                                  CodeSnippetType
                                      .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                      .build()));

      StreamObserver<GetAllCustomTestPluginsResponse> responseStreamObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () -> astConfigService.getAllCustomTestPlugins(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(
              GetAllCustomTestPluginsResponse.newBuilder()
                  .addAllCustomTestPlugins(
                      List.of(
                          CustomTestPlugin.newBuilder()
                              .setId("id1")
                              .setName("custom-test-matching")
                              .setPluginScope(
                                  PluginScope.newBuilder()
                                      .setEnvironmentScope(
                                          EnvironmentScope.newBuilder()
                                              .addAllEnvironmentIds(List.of("env1"))))
                              .setCodeSnippetDetails(
                                  CodeSnippetDetails.newBuilder()
                                      .setCodeSnippet("code-snippet")
                                      .setCodeSnippetType(
                                          CodeSnippetType
                                              .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                              .addAllSupportedApiTypes(DEFAULT_SUPPORTED_API_TYPES)
                              .build()))
                  .build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("Should fail on invalid request")
    void should_fail_on_invalid_request() {
      GetAllCustomTestPluginsRequest request = GetAllCustomTestPluginsRequest.newBuilder().build();

      doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
          .when(requestValidator)
          .validateOrThrow(any(), eq(request));

      StreamObserver<GetAllCustomTestPluginsResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () -> astConfigService.getAllCustomTestPlugins(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }
  }

  @Nested
  class CreateCustomTestPlugin {
    @Test
    @DisplayName("Should create custom test plugin on valid request")
    void should_create_custom_test_plugin() {
      CreateCustomTestPluginRequest request =
          CreateCustomTestPluginRequest.newBuilder()
              .setCreateCustomTestPlugin(
                  ai.traceable.ast.config.service.v1.CreateCustomTestPlugin.newBuilder()
                      .setName("custom-test")
                      .setCodeSnippetDetails(
                          CodeSnippetDetails.newBuilder()
                              .setCodeSnippet("code-snippet")
                              .setCodeSnippetType(
                                  CodeSnippetType
                                      .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                      .addAllSupportedApiTypes(List.of(API_TYPE_HTTP)))
              .build();

      CustomTestPlugin customTestPlugin =
          CustomTestPlugin.newBuilder()
              .setName("custom-test")
              .setId("test-Id")
              .setCodeSnippetDetails(
                  CodeSnippetDetails.newBuilder()
                      .setCodeSnippet("code-snippet")
                      .setCodeSnippetType(
                          CodeSnippetType
                              .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
              .addAllSupportedApiTypes(List.of(API_TYPE_HTTP))
              .build();

      ContextualConfigObject<CustomTestPlugin> contextualConfigObject =
          mock(ContextualConfigObject.class);
      when(contextualConfigObject.getContext()).thenReturn("context");
      when(contextualConfigObject.getData()).thenReturn(customTestPlugin);

      when(customTestPluginStore.upsertObject(any(), any())).thenReturn(contextualConfigObject);

      StreamObserver<CreateCustomTestPluginResponse> responseStreamObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () -> astConfigService.createCustomTestPlugin(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(
              CreateCustomTestPluginResponse.newBuilder()
                  .setCustomTestPlugin(
                      CustomTestPlugin.newBuilder()
                          .setName("custom-test")
                          .setId("test-Id")
                          .setCodeSnippetDetails(
                              CodeSnippetDetails.newBuilder()
                                  .setCodeSnippet("code-snippet")
                                  .setCodeSnippetType(
                                      CodeSnippetType
                                          .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                          .addSupportedApiTypes(API_TYPE_HTTP))
                  .build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("Should fail on invalid request")
    void should_fail_on_invalid_request() {
      CreateCustomTestPluginRequest request =
          CreateCustomTestPluginRequest.newBuilder()
              .setCreateCustomTestPlugin(
                  ai.traceable.ast.config.service.v1.CreateCustomTestPlugin.newBuilder()
                      .setName("test-name")
                      .setCodeSnippetDetails(
                          CodeSnippetDetails.newBuilder()
                              .setCodeSnippet("code-snippet")
                              .setCodeSnippetType(CodeSnippetType.CODE_SNIPPET_TYPE_UNSPECIFIED)))
              .build();

      doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
          .when(requestValidator)
          .validateOrThrow(any(), eq(request));

      StreamObserver<CreateCustomTestPluginResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () -> astConfigService.createCustomTestPlugin(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }
  }

  @Nested
  class UpdateCustomTestPlugin {
    @Test
    @DisplayName("Should update custom test plugin on valid request")
    void should_update_custom_test_plugin() {
      UpdateCustomTestPluginRequest request =
          UpdateCustomTestPluginRequest.newBuilder()
              .setUpdateCustomTestPlugin(
                  ai.traceable.ast.config.service.v1.UpdateCustomTestPlugin.newBuilder()
                      .setId("test-id")
                      .setName("test-name")
                      .setCodeSnippetDetails(
                          CodeSnippetDetails.newBuilder()
                              .setCodeSnippet("code-snippet")
                              .setCodeSnippetType(
                                  CodeSnippetType
                                      .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                      .addSupportedApiTypes(API_TYPE_HTTP))
              .build();

      CustomTestPlugin customTestPlugin =
          CustomTestPlugin.newBuilder()
              .setName("custom-test")
              .setId("test-Id")
              .setCodeSnippetDetails(
                  CodeSnippetDetails.newBuilder()
                      .setCodeSnippet("code-snippet")
                      .setCodeSnippetType(
                          CodeSnippetType
                              .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
              .addSupportedApiTypes(API_TYPE_HTTP)
              .build();

      ContextualConfigObject<CustomTestPlugin> contextualConfigObject =
          mock(ContextualConfigObject.class);
      when(contextualConfigObject.getContext()).thenReturn("context");
      when(contextualConfigObject.getData()).thenReturn(customTestPlugin);

      when(customTestPluginStore.getData(any(), any())).thenReturn(Optional.of(customTestPlugin));
      when(customTestPluginStore.upsertObject(any(), any())).thenReturn(contextualConfigObject);

      StreamObserver<UpdateCustomTestPluginResponse> responseStreamObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () -> astConfigService.updateCustomTestPlugin(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(
              UpdateCustomTestPluginResponse.newBuilder()
                  .setCustomTestPlugin(
                      CustomTestPlugin.newBuilder()
                          .setName("custom-test")
                          .setId("test-Id")
                          .setCodeSnippetDetails(
                              CodeSnippetDetails.newBuilder()
                                  .setCodeSnippet("code-snippet")
                                  .setCodeSnippetType(
                                      CodeSnippetType
                                          .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT))
                          .addSupportedApiTypes(API_TYPE_HTTP))
                  .build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("Should fail on invalid request")
    void should_fail_on_invalid_request() {
      UpdateCustomTestPluginRequest request =
          UpdateCustomTestPluginRequest.newBuilder()
              .setUpdateCustomTestPlugin(
                  ai.traceable.ast.config.service.v1.UpdateCustomTestPlugin.newBuilder()
                      .setName(""))
              .build();
      doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
          .when(requestValidator)
          .validateOrThrow(any(), eq(request));

      StreamObserver<UpdateCustomTestPluginResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () -> astConfigService.updateCustomTestPlugin(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }
  }

  @Nested
  class DeleteCustomTestPlugin {
    @Test
    @DisplayName("Should delete custom test plugin with given id on valid request")
    void should_delete_custom_test_plugin() {
      DeleteCustomTestPluginRequest request =
          DeleteCustomTestPluginRequest.newBuilder().setId("test-id").build();

      DeletedContextualConfigObject<CustomTestPlugin> deletedContextualConfigObject =
          mock(DeletedContextualConfigObject.class);

      when(customTestPluginStore.deleteObject(any(), eq("test-id")))
          .thenReturn(Optional.ofNullable(deletedContextualConfigObject));

      StreamObserver<DeleteCustomTestPluginResponse> responseStreamObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () -> astConfigService.deleteCustomTestPlugin(request, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(DeleteCustomTestPluginResponse.getDefaultInstance());

      verify(responseStreamObserver, times(1)).onCompleted();
    }
  }

  @Test
  void testAstOverridesCrud() {
    final Timestamp mockTimestamp = Timestamp.newBuilder().setSeconds(100L).build();
    when(timestampConverter.convert(any())).thenReturn(mockTimestamp);
    final AstOverrideInfo astOverrideInfo1 =
        AstOverrideInfo.newBuilder()
            .setName("name1")
            .setDescription("description")
            .addScopes(OverrideScope.newBuilder().setFixedTestId("fixedTestId1"))
            .setConfig(
                OverrideConfig.newBuilder()
                    .setMutationOverride(
                        MutationOverride.newBuilder()
                            .setSystemDefinedMutationOverride(
                                SystemDefinedMutationOverride.getDefaultInstance())))
            .build();
    final AstOverrideInfo astOverrideInfo2 =
        AstOverrideInfo.newBuilder()
            .setName("name2")
            .setDescription("description")
            .addScopes(OverrideScope.newBuilder().setFixedTestId("fixedTestId2"))
            .setConfig(
                OverrideConfig.newBuilder()
                    .setMutationOverride(
                        MutationOverride.newBuilder()
                            .setSystemDefinedMutationOverride(
                                SystemDefinedMutationOverride.getDefaultInstance())))
            .build();

    AstOverride createdAstOverride1 =
        astConfigServiceBlockingStub
            .createAstOverride(
                CreateAstOverrideRequest.newBuilder().setAstOverrideInfo(astOverrideInfo1).build())
            .getAstOverride();
    GetAstOverridesResponse getAstOverridesResponse =
        astConfigServiceBlockingStub.getAstOverrides(GetAstOverridesRequest.getDefaultInstance());
    assertEquals(1, getAstOverridesResponse.getAstOverridesCount());

    AstOverride createdAstOverride2 =
        astConfigServiceBlockingStub
            .createAstOverride(
                CreateAstOverrideRequest.newBuilder().setAstOverrideInfo(astOverrideInfo2).build())
            .getAstOverride();
    // test id filter
    getAstOverridesResponse =
        astConfigServiceBlockingStub.getAstOverrides(
            GetAstOverridesRequest.newBuilder()
                .setFilter(
                    AstOverrideFilter.newBuilder()
                        .setIdFilter(IdFilter.newBuilder().addIds(createdAstOverride1.getId())))
                .build());
    assertEquals(List.of(createdAstOverride1), getAstOverridesResponse.getAstOverridesList());
    getAstOverridesResponse =
        astConfigServiceBlockingStub.getAstOverrides(GetAstOverridesRequest.getDefaultInstance());
    assertEquals(2, getAstOverridesResponse.getAstOverridesCount());

    assertTrue(
        getAstOverridesResponse
            .getAstOverridesList()
            .containsAll(
                List.of(
                    AstOverride.newBuilder()
                        .setId(createdAstOverride1.getId())
                        .setName("name1")
                        .setDescription("description")
                        .addScopes(OverrideScope.newBuilder().setFixedTestId("fixedTestId1"))
                        .setConfig(
                            OverrideConfig.newBuilder()
                                .setMutationOverride(
                                    MutationOverride.newBuilder()
                                        .setSystemDefinedMutationOverride(
                                            SystemDefinedMutationOverride.getDefaultInstance())))
                        .setLastUpdatedTimestamp(mockTimestamp)
                        .build(),
                    AstOverride.newBuilder()
                        .setId(createdAstOverride2.getId())
                        .setName("name2")
                        .setDescription("description")
                        .addScopes(OverrideScope.newBuilder().setFixedTestId("fixedTestId2"))
                        .setConfig(
                            OverrideConfig.newBuilder()
                                .setMutationOverride(
                                    MutationOverride.newBuilder()
                                        .setSystemDefinedMutationOverride(
                                            SystemDefinedMutationOverride.getDefaultInstance())))
                        .setLastUpdatedTimestamp(mockTimestamp)
                        .build())));

    astConfigServiceBlockingStub.updateAstOverride(
        UpdateAstOverrideRequest.newBuilder()
            .setId(createdAstOverride1.getId())
            .setAstOverrideInfo(
                AstOverrideInfo.newBuilder()
                    .setName("updatedName")
                    .setDescription("updatedDescription")
                    .addScopes(OverrideScope.newBuilder().setFixedTestId("fixedTestId2"))
                    .setConfig(
                        OverrideConfig.newBuilder()
                            .setMutationOverride(
                                MutationOverride.newBuilder()
                                    .setSystemDefinedMutationOverride(
                                        SystemDefinedMutationOverride.getDefaultInstance())))
                    .build())
            .build());
    getAstOverridesResponse =
        astConfigServiceBlockingStub.getAstOverrides(GetAstOverridesRequest.getDefaultInstance());
    assertEquals(2, getAstOverridesResponse.getAstOverridesCount());
    assertTrue(
        getAstOverridesResponse
            .getAstOverridesList()
            .contains(
                AstOverride.newBuilder()
                    .setId(createdAstOverride1.getId())
                    .setName("updatedName")
                    .setDescription("updatedDescription")
                    .addScopes(OverrideScope.newBuilder().setFixedTestId("fixedTestId2"))
                    .setConfig(
                        OverrideConfig.newBuilder()
                            .setMutationOverride(
                                MutationOverride.newBuilder()
                                    .setSystemDefinedMutationOverride(
                                        SystemDefinedMutationOverride.getDefaultInstance())))
                    .setLastUpdatedTimestamp(mockTimestamp)
                    .build()));
    astConfigServiceBlockingStub.deleteAstOverrides(
        DeleteAstOverridesRequest.newBuilder().addIds(createdAstOverride1.getId()).build());
    getAstOverridesResponse =
        astConfigServiceBlockingStub.getAstOverrides(GetAstOverridesRequest.getDefaultInstance());
    assertEquals(List.of(createdAstOverride2), getAstOverridesResponse.getAstOverridesList());
  }
}
