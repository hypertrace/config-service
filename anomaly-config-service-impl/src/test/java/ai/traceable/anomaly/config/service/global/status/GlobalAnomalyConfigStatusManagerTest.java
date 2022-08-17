package ai.traceable.anomaly.config.service.global.status;

import static ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_HIGH;
import static ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_LOW;
import static ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_MEDIUM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.common.license.LicenseInfoLoader;
import ai.traceable.anomaly.config.service.common.license.LicenseMeteringServiceConfig;
import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConfig;
import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConstants;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.global.ExcludedEventsGenerationConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import ai.traceable.license.metering.service.api.v1.GetLicenseInfoRequest;
import ai.traceable.license.metering.service.api.v1.GetLicenseInfoResponse;
import ai.traceable.license.metering.service.api.v1.LicenseInfo;
import ai.traceable.license.metering.service.api.v1.LicenseMeteringServiceGrpc;
import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GlobalAnomalyConfigStatusManagerTest {

  private static Server mockServer;
  private static MockGenericConfigService mockConfigService;
  private static Channel channelForMockServer;
  private AnomalyGlobalConfigServiceConfig config;
  private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private LicenseInfoLoader licenseInfoLoader;
  private ScopedGlobalConfigStatusChangeConverter configConverter;

  private GlobalAnomalyConfigStatusManagerImpl configStatusManager;

  private final AnomalyServiceScope serviceScope =
      AnomalyServiceScope.newBuilder().setId("service").build();
  private final AnomalyApiScope apiScope =
      AnomalyApiScope.newBuilder().setId("api").setServiceScope(serviceScope).build();

  private final AnomalyConfigScope customerConfigScope =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();
  private final AnomalyConfigScope serviceConfigScope =
      AnomalyConfigScope.newBuilder().setServiceScope(serviceScope).build();
  private final AnomalyConfigScope apiConfigScope =
      AnomalyConfigScope.newBuilder().setApiScope(apiScope).build();

  @BeforeAll
  public static void setupServer() throws IOException {
    TestInterceptor testInterceptor = new TestInterceptor();

    String serverName = InProcessServerBuilder.generateName();
    mockServer =
        InProcessServerBuilder.forName(serverName)
            .addService(
                ServerInterceptors.intercept(new MockLicenseMeteringService(), testInterceptor))
            .build()
            .start();
    channelForMockServer = InProcessChannelBuilder.forName(serverName).directExecutor().build();

    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
  }

  @BeforeEach
  public void setup() {
    configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    licenseInfoLoader =
        new LicenseInfoLoader(
            new LicenseMeteringServiceConfig(
                ConfigFactory.parseString(
                    "host = \"localhost\"\n"
                        + "  port = 51018\n"
                        + "  call.timeout.duration = 60000\n"
                        + "  cache.expiration.duration = 5m\n"
                        + "  cache.max.size = 5000")),
            LicenseMeteringServiceGrpc.newBlockingStub(channelForMockServer)
                .withCallCredentials(
                    RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider()
                        .get()));

    config =
        new AnomalyGlobalConfigServiceConfig(
            ConfigFactory.parseString(
                "disabled = true\n"
                    + "  internal = false\n"
                    + "  minConfidenceLevel = ANOMALY_CONFIDENCE_LEVEL_MEDIUM\n"
                    + "  licenseTiers = [\n"
                    + "    {\n"
                    + "        tier = TIER_TEAM_TRIAL\n"
                    + "        disabled = false\n"
                    + "    }\n"
                    + "  ]\n"));
    configConverter = new ScopedGlobalConfigStatusChangeConverter();
    this.configStatusManager =
        spy(
            new GlobalAnomalyConfigStatusManagerImpl(
                config,
                configServiceBlockingStub,
                configConverter,
                new AnomalyConfigScopeUtils(),
                licenseInfoLoader));
  }

  @AfterAll
  public static void teardown() {
    mockConfigService.shutdown();
    mockServer.shutdownNow();
  }

  @Test
  public void test_getScopedAnomalyConfigStatus() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);

    AnomalyConfigStatusChange configStatusChange;
    AnomalyConfigStatus expectedCustomerStatus;
    AnomalyConfigStatus expectedServiceStatus;
    AnomalyConfigStatus expectedApiStatus;
    List<ScopedAnomalyConfigStatus> scopedConfigs;

    assertThrows(
        RuntimeException.class,
        () ->
            configStatusManager.getScopedAnomalyConfigStatus(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build()));

    {
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder()
              .setInternal(false)
              .setDisabled(true)
              .build(); // default status
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
              .getConfigStatus());
      scopedConfigs = configStatusManager.getAllScopedAnomalyConfigStatusConfigs(requestContext);
      assertEquals(1, scopedConfigs.size());
      assertEquals(customerConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(ANOMALY_CONFIDENCE_LEVEL_MEDIUM, scopedConfigs.get(0).getMinConfidenceLevel());
    }
    {
      RequestContext teamTrialRequestContext =
          RequestContext.forTenantId(tenantId + "_" + LicenseInfo.Tier.TIER_TEAM_TRIAL);
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(teamTrialRequestContext, customerConfigScope)
              .getConfigStatus());
      scopedConfigs =
          configStatusManager.getAllScopedAnomalyConfigStatusConfigs(teamTrialRequestContext);
      assertEquals(1, scopedConfigs.size());
      assertEquals(customerConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(ANOMALY_CONFIDENCE_LEVEL_MEDIUM, scopedConfigs.get(0).getMinConfidenceLevel());
    }
    {
      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(false).build();
      ScopedAnomalyConfigStatusChange customerScopedConfig =
          upsertCustomerConfigStatus(configStatusChange, tenantId);
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build();
      ScopedAnomalyConfigStatus scopedAnomalyConfigStatus;

      scopedAnomalyConfigStatus =
          configStatusManager.getScopedAnomalyConfigStatus(requestContext, customerConfigScope);
      assertEquals(expectedCustomerStatus, scopedAnomalyConfigStatus.getConfigStatus());
      assertEquals(
          customerScopedConfig.getMinConfidenceLevel(),
          scopedAnomalyConfigStatus.getMinConfidenceLevel());
      assertEquals(
          customerScopedConfig.getExcludedEventsConfig(),
          scopedAnomalyConfigStatus.getExcludedEventsConfig());

      scopedAnomalyConfigStatus =
          configStatusManager.getScopedAnomalyConfigStatus(requestContext, serviceConfigScope);
      assertEquals(expectedCustomerStatus, scopedAnomalyConfigStatus.getConfigStatus());
      assertEquals(
          customerScopedConfig.getMinConfidenceLevel(),
          scopedAnomalyConfigStatus.getMinConfidenceLevel());
      assertEquals(
          customerScopedConfig.getExcludedEventsConfig(),
          scopedAnomalyConfigStatus.getExcludedEventsConfig());

      scopedAnomalyConfigStatus =
          configStatusManager.getScopedAnomalyConfigStatus(requestContext, apiConfigScope);
      assertEquals(expectedCustomerStatus, scopedAnomalyConfigStatus.getConfigStatus());
      assertEquals(
          customerScopedConfig.getMinConfidenceLevel(),
          scopedAnomalyConfigStatus.getMinConfidenceLevel());
      assertEquals(
          customerScopedConfig.getExcludedEventsConfig(),
          scopedAnomalyConfigStatus.getExcludedEventsConfig());

      scopedConfigs = configStatusManager.getAllScopedAnomalyConfigStatusConfigs(requestContext);
      assertEquals(1, scopedConfigs.size());
      assertEquals(customerConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(
          customerScopedConfig.getMinConfidenceLevel(),
          scopedConfigs.get(0).getMinConfidenceLevel());
      assertEquals(
          customerScopedConfig.getExcludedEventsConfig(),
          scopedConfigs.get(0).getExcludedEventsConfig());
    }
    {
      configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(true).build();
      ScopedAnomalyConfigStatusChange apiScopedConfig = upsertApiConfigStatus(configStatusChange);
      expectedApiStatus =
          AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(false).build();
      ScopedAnomalyConfigStatus scopedAnomalyConfigStatus;

      scopedAnomalyConfigStatus =
          configStatusManager.getScopedAnomalyConfigStatus(requestContext, apiConfigScope);
      assertEquals(expectedApiStatus, scopedAnomalyConfigStatus.getConfigStatus());
      assertEquals(
          apiScopedConfig.getMinConfidenceLevel(),
          scopedAnomalyConfigStatus.getMinConfidenceLevel());
      assertEquals(
          apiScopedConfig.getExcludedEventsConfig(),
          scopedAnomalyConfigStatus.getExcludedEventsConfig());

      // customer config stays unchanged..
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build();
      ExcludedEventsGenerationConfig expectedCustomerExcludedEventsConfig =
          ExcludedEventsGenerationConfig.newBuilder().setEnabledForAll(true).build();
      AnomalyConfidenceLevel expectedCustomerMinConfidenceLevel = ANOMALY_CONFIDENCE_LEVEL_HIGH;

      scopedAnomalyConfigStatus =
          configStatusManager.getScopedAnomalyConfigStatus(requestContext, customerConfigScope);
      assertEquals(expectedCustomerStatus, scopedAnomalyConfigStatus.getConfigStatus());
      assertEquals(
          expectedCustomerMinConfidenceLevel, scopedAnomalyConfigStatus.getMinConfidenceLevel());
      assertEquals(
          expectedCustomerExcludedEventsConfig,
          scopedAnomalyConfigStatus.getExcludedEventsConfig());

      scopedAnomalyConfigStatus =
          configStatusManager.getScopedAnomalyConfigStatus(requestContext, serviceConfigScope);
      assertEquals(expectedCustomerStatus, scopedAnomalyConfigStatus.getConfigStatus());
      assertEquals(
          expectedCustomerMinConfidenceLevel, scopedAnomalyConfigStatus.getMinConfidenceLevel());
      assertEquals(
          expectedCustomerExcludedEventsConfig,
          scopedAnomalyConfigStatus.getExcludedEventsConfig());

      scopedConfigs = configStatusManager.getAllScopedAnomalyConfigStatusConfigs(requestContext);
      assertEquals(2, scopedConfigs.size());
      assertEquals(apiConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedApiStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(
          apiScopedConfig.getMinConfidenceLevel(), scopedConfigs.get(0).getMinConfidenceLevel());
      assertEquals(
          apiScopedConfig.getExcludedEventsConfig(),
          scopedConfigs.get(0).getExcludedEventsConfig());
      assertEquals(customerConfigScope, scopedConfigs.get(1).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(1).getConfigStatus());
      assertEquals(
          expectedCustomerMinConfidenceLevel, scopedConfigs.get(1).getMinConfidenceLevel());
      assertEquals(
          expectedCustomerExcludedEventsConfig, scopedConfigs.get(1).getExcludedEventsConfig());
    }
    {
      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
      upsertServiceConfigStatus(configStatusChange);
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build();
      expectedServiceStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build();
      expectedApiStatus =
          AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
              .getConfigStatus());
      assertEquals(
          expectedServiceStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, serviceConfigScope)
              .getConfigStatus());
      assertEquals(
          expectedApiStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, apiConfigScope)
              .getConfigStatus());
      scopedConfigs = configStatusManager.getAllScopedAnomalyConfigStatusConfigs(requestContext);
      assertEquals(3, scopedConfigs.size());
      assertEquals(serviceConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedServiceStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(apiConfigScope, scopedConfigs.get(1).getConfigScope());
      assertEquals(expectedApiStatus, scopedConfigs.get(1).getConfigStatus());
      assertEquals(customerConfigScope, scopedConfigs.get(2).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(2).getConfigStatus());
    }
  }

  @Test
  public void test_updateAnomalyConfigStatus() {
    RequestContext requestContext = RequestContext.forTenantId("update_tenant");

    assertThrows(
        RuntimeException.class,
        () ->
            updateAnomalyConfigStatus(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build(),
                AnomalyConfigStatusChange.getDefaultInstance()));

    AnomalyConfigStatusChange configStatusChange;
    AnomalyConfigStatus expectedCustomerStatus;
    {
      configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(true).build();
      assertEquals(
          configStatusChange,
          updateAnomalyConfigStatus(requestContext, customerConfigScope, configStatusChange));
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
              .getConfigStatus());
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, serviceConfigScope)
              .getConfigStatus());
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, apiConfigScope)
              .getConfigStatus());

      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(false).build();
      assertEquals(
          AnomalyConfigStatusChange.newBuilder().setDisabled(false).setInternal(true).build(),
          updateAnomalyConfigStatus(requestContext, customerConfigScope, configStatusChange));
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setDisabled(false).setInternal(true).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
              .getConfigStatus());

      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
      assertEquals(
          AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build(),
          updateAnomalyConfigStatus(requestContext, customerConfigScope, configStatusChange));
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setDisabled(true).setInternal(true).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
              .getConfigStatus());
    }

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
    assertEquals(
        configStatusChange,
        updateAnomalyConfigStatus(requestContext, serviceConfigScope, configStatusChange));
    AnomalyConfigStatus expectedServiceStatus =
        AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build();
    assertEquals(
        expectedCustomerStatus,
        configStatusManager
            .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
            .getConfigStatus());
    assertEquals(
        expectedServiceStatus,
        configStatusManager
            .getScopedAnomalyConfigStatus(requestContext, serviceConfigScope)
            .getConfigStatus());
    assertEquals(
        expectedServiceStatus,
        configStatusManager
            .getScopedAnomalyConfigStatus(requestContext, apiConfigScope)
            .getConfigStatus());

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(false).build();
    assertEquals(
        configStatusChange,
        updateAnomalyConfigStatus(requestContext, apiConfigScope, configStatusChange));
    AnomalyConfigStatus expectedApiStatus =
        AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build();
    assertEquals(
        expectedCustomerStatus,
        configStatusManager
            .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
            .getConfigStatus());
    assertEquals(
        expectedServiceStatus,
        configStatusManager
            .getScopedAnomalyConfigStatus(requestContext, serviceConfigScope)
            .getConfigStatus());
    assertEquals(
        expectedApiStatus,
        configStatusManager
            .getScopedAnomalyConfigStatus(requestContext, apiConfigScope)
            .getConfigStatus());
  }

  private ScopedAnomalyConfigStatusChange upsertApiConfigStatus(
      AnomalyConfigStatusChange configStatus) throws InvalidProtocolBufferException {
    ScopedAnomalyConfigStatusChange scopedConfig =
        ScopedAnomalyConfigStatusChange.newBuilder()
            .setConfigStatus(configStatus)
            .setConfigScope(apiConfigScope)
            .setExcludedEventsConfig(
                ExcludedEventsGenerationConfig.newBuilder()
                    .setExclusionRuleIds(
                        StringList.newBuilder().addAllValues(List.of("rule1", "rule2"))))
            .setMinConfidenceLevel(ANOMALY_CONFIDENCE_LEVEL_LOW)
            .build();
    configServiceBlockingStub.upsertConfig(
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(
                AnomalyGlobalConfigServiceConstants.GLOBAL_ANOMALY_CONFIG_NAMESPACE)
            .setResourceName(
                AnomalyGlobalConfigServiceConstants.GLOBAL_ANOMALY_CONFIG_STATUS_RESOURCE_NAME)
            .setConfig(configConverter.convert(scopedConfig))
            .setContext(apiScope.getId())
            .build());
    return scopedConfig;
  }

  private void upsertServiceConfigStatus(AnomalyConfigStatusChange configStatus)
      throws InvalidProtocolBufferException {
    configServiceBlockingStub.upsertConfig(
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(
                AnomalyGlobalConfigServiceConstants.GLOBAL_ANOMALY_CONFIG_NAMESPACE)
            .setResourceName(
                AnomalyGlobalConfigServiceConstants.GLOBAL_ANOMALY_CONFIG_STATUS_RESOURCE_NAME)
            .setConfig(
                configConverter.convert(
                    ScopedAnomalyConfigStatusChange.newBuilder()
                        .setConfigStatus(configStatus)
                        .setConfigScope(serviceConfigScope)
                        .build()))
            .setContext(serviceScope.getId())
            .build());
  }

  private ScopedAnomalyConfigStatusChange upsertCustomerConfigStatus(
      AnomalyConfigStatusChange configStatus, String tenantId)
      throws InvalidProtocolBufferException {
    ScopedAnomalyConfigStatusChange scopedConfig =
        ScopedAnomalyConfigStatusChange.newBuilder()
            .setConfigStatus(configStatus)
            .setConfigScope(customerConfigScope)
            .setExcludedEventsConfig(
                ExcludedEventsGenerationConfig.newBuilder().setEnabledForAll(true))
            .setMinConfidenceLevel(ANOMALY_CONFIDENCE_LEVEL_HIGH)
            .build();
    RequestContext.forTenantId(tenantId)
        .call(
            () ->
                configServiceBlockingStub.upsertConfig(
                    UpsertConfigRequest.newBuilder()
                        .setContext(tenantId)
                        .setResourceNamespace(
                            AnomalyGlobalConfigServiceConstants.GLOBAL_ANOMALY_CONFIG_NAMESPACE)
                        .setResourceName(
                            AnomalyGlobalConfigServiceConstants
                                .GLOBAL_ANOMALY_CONFIG_STATUS_RESOURCE_NAME)
                        .setConfig(configConverter.convert(scopedConfig))
                        .build()));
    return scopedConfig;
  }

  private AnomalyConfigStatusChange updateAnomalyConfigStatus(
      RequestContext requestContext, AnomalyConfigScope scope, AnomalyConfigStatusChange status) {
    ScopedAnomalyConfigStatusChange scopedConfig =
        requestContext.call(
            () ->
                configStatusManager.updateScopedAnomalyConfigStatus(
                    requestContext,
                    ScopedAnomalyConfigStatusChange.newBuilder()
                        .setConfigScope(scope)
                        .setConfigStatus(status)
                        .build()));
    assertEquals(scope, scopedConfig.getConfigScope());
    return scopedConfig.getConfigStatus();
  }

  protected static class MockLicenseMeteringService
      extends LicenseMeteringServiceGrpc.LicenseMeteringServiceImplBase {
    private static final String TENANT_ID_PREFIX = "tenant_";
    private static final int TENANT_ID_PREFIX_LENGTH = TENANT_ID_PREFIX.length();

    @Override
    public void getLicenseInfo(
        GetLicenseInfoRequest request,
        StreamObserver<GetLicenseInfoResponse> responseStreamObserver) {
      String tenantId = RequestContext.CURRENT.get().getTenantId().get();
      if (tenantId.startsWith(TENANT_ID_PREFIX)) {
        responseStreamObserver.onNext(
            GetLicenseInfoResponse.newBuilder()
                .setLicenseInfo(
                    LicenseInfo.newBuilder()
                        .setTier(
                            LicenseInfo.Tier.valueOf(tenantId.substring(TENANT_ID_PREFIX_LENGTH))))
                .build());
        responseStreamObserver.onCompleted();
        return;
      }
      responseStreamObserver.onError(Status.NOT_FOUND.asException());
    }
  }

  private static class TestInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
      Context ctx =
          Context.current()
              .withValue(
                  RequestContext.CURRENT,
                  RequestContext.forTenantId(
                      headers.get(
                          Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER))));
      return Contexts.interceptCall(ctx, call, headers, next);
    }
  }
}
