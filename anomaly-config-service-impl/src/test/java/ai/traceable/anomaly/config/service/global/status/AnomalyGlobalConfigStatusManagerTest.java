package ai.traceable.anomaly.config.service.global.status;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;

import ai.traceable.anomaly.config.service.common.license.LicenseInfoLoader;
import ai.traceable.anomaly.config.service.common.license.LicenseMeteringServiceConfig;
import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConfig;
import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConstants;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScopeType;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
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
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AnomalyGlobalConfigStatusManagerTest {

  private static Server mockServer;
  private static MockGenericConfigService mockConfigService;
  private static Channel channelForMockServer;
  private AnomalyGlobalConfigServiceConfig config;
  private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private LicenseInfoLoader licenseInfoLoader;
  private GlobalConfigStatusConverter configConverter;

  private AnomalyGlobalConfigStatusManager configStatusManager;

  private final AnomalyServiceScope serviceScope =
      AnomalyServiceScope.newBuilder().setId("service").build();
  private final AnomalyApiScope apiScope =
      AnomalyApiScope.newBuilder().setId("api").setServiceScope(serviceScope).build();

  private final AnomalyConfigScope customerConfigScope =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();
  private final AnomalyConfigScope serviceConfigScope =
      AnomalyConfigScope.newBuilder()
          .setScopeType(AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_SERVICE)
          .setServiceScope(serviceScope)
          .build();
  private final AnomalyConfigScope apiConfigScope =
      AnomalyConfigScope.newBuilder()
          .setScopeType(AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_API)
          .setApiScope(apiScope)
          .build();

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

    channelForMockServer = InProcessChannelBuilder.forName(serverName).build();

    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
  }

  @BeforeEach
  public void setup() {
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    licenseInfoLoader =
        new LicenseInfoLoader(
            new LicenseMeteringServiceConfig(
                ConfigFactory.parseString(
                    "host = \"localhost\"\n"
                        + "  port = 51018\n"
                        + "  call.timeout.ms = 60000\n"
                        + "  cache.expiry.duration = 5m\n"
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
                    + "  licenseTiers = [\n"
                    + "    {\n"
                    + "        tier = TIER_TEAM_TRIAL\n"
                    + "        disabled = false\n"
                    + "    }\n"
                    + "  ]\n"));
    configConverter = new GlobalConfigStatusConverter();
    this.configStatusManager =
        spy(
            new AnomalyGlobalConfigStatusManager(
                config, configServiceBlockingStub, configConverter, licenseInfoLoader));
  }

  @AfterAll
  public static void teardown() {
    mockConfigService.shutdown();
    mockServer.shutdownNow();
  }

  @Test
  public void testGetAnomalyConfigStatus() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);

    AnomalyConfigStatusChange configStatusChange;
    AnomalyConfigStatus expectedStatus;

    assertThrows(
        RuntimeException.class,
        () ->
            configStatusManager.getAnomalyConfigStatus(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build()));

    assertEquals(
        AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build(),
        configStatusManager.getAnomalyConfigStatus(
            RequestContext.forTenantId(tenantId + "_" + LicenseInfo.Tier.TIER_TEAM_TRIAL),
            customerConfigScope));

    expectedStatus =
        AnomalyConfigStatus.newBuilder()
            .setInternal(false)
            .setDisabled(true)
            .build(); // default status
    assertEquals(
        expectedStatus,
        configStatusManager.getAnomalyConfigStatus(requestContext, customerConfigScope));

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(false).build();
    upsertCustomerConfigStatus(configStatusChange, tenantId);
    expectedStatus = AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build();
    assertEquals(
        expectedStatus,
        configStatusManager.getAnomalyConfigStatus(requestContext, customerConfigScope));
    assertEquals(
        expectedStatus,
        configStatusManager.getAnomalyConfigStatus(requestContext, serviceConfigScope));
    assertEquals(
        expectedStatus, configStatusManager.getAnomalyConfigStatus(requestContext, apiConfigScope));

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(true).build();
    upsertApiConfigStatus(configStatusChange);
    expectedStatus = AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(false).build();
    assertEquals(
        expectedStatus, configStatusManager.getAnomalyConfigStatus(requestContext, apiConfigScope));
    // customer config stays unchanged..
    expectedStatus = AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build();
    assertEquals(
        expectedStatus,
        configStatusManager.getAnomalyConfigStatus(requestContext, customerConfigScope));
    assertEquals(
        expectedStatus,
        configStatusManager.getAnomalyConfigStatus(requestContext, serviceConfigScope));

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
    upsertServiceConfigStatus(configStatusChange);
    assertEquals(
        AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build(),
        configStatusManager.getAnomalyConfigStatus(requestContext, customerConfigScope));
    assertEquals(
        AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build(),
        configStatusManager.getAnomalyConfigStatus(requestContext, serviceConfigScope));
    assertEquals(
        AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build(),
        configStatusManager.getAnomalyConfigStatus(requestContext, apiConfigScope));
  }

  @Test
  public void testUpdateAnomalyConfigStatus() {
    RequestContext requestContext = RequestContext.forTenantId("update_tenant");
    assertThrows(
        RuntimeException.class,
        () ->
            configStatusManager.updateAnomalyConfigStatus(
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
          configStatusManager.updateAnomalyConfigStatus(
              requestContext, customerConfigScope, configStatusChange));
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager.getAnomalyConfigStatus(requestContext, customerConfigScope));
      assertEquals(
          expectedCustomerStatus,
          configStatusManager.getAnomalyConfigStatus(requestContext, serviceConfigScope));
      assertEquals(
          expectedCustomerStatus,
          configStatusManager.getAnomalyConfigStatus(requestContext, apiConfigScope));

      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(false).build();
      assertEquals(
          AnomalyConfigStatusChange.newBuilder().setDisabled(false).setInternal(true).build(),
          configStatusManager.updateAnomalyConfigStatus(
              requestContext, customerConfigScope, configStatusChange));
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setDisabled(false).setInternal(true).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager.getAnomalyConfigStatus(requestContext, customerConfigScope));

      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
      assertEquals(
          AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build(),
          configStatusManager.updateAnomalyConfigStatus(
              requestContext, customerConfigScope, configStatusChange));
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setDisabled(true).setInternal(true).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager.getAnomalyConfigStatus(requestContext, customerConfigScope));
    }

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
    assertEquals(
        configStatusChange,
        configStatusManager.updateAnomalyConfigStatus(
            requestContext, serviceConfigScope, configStatusChange));
    AnomalyConfigStatus expectedServiceStatus =
        AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build();
    assertEquals(
        expectedCustomerStatus,
        configStatusManager.getAnomalyConfigStatus(requestContext, customerConfigScope));
    assertEquals(
        expectedServiceStatus,
        configStatusManager.getAnomalyConfigStatus(requestContext, serviceConfigScope));
    assertEquals(
        expectedServiceStatus,
        configStatusManager.getAnomalyConfigStatus(requestContext, apiConfigScope));

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(false).build();
    assertEquals(
        configStatusChange,
        configStatusManager.updateAnomalyConfigStatus(
            requestContext, apiConfigScope, configStatusChange));
    AnomalyConfigStatus expectedApiStatus =
        AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build();
    assertEquals(
        expectedCustomerStatus,
        configStatusManager.getAnomalyConfigStatus(requestContext, customerConfigScope));
    assertEquals(
        expectedServiceStatus,
        configStatusManager.getAnomalyConfigStatus(requestContext, serviceConfigScope));
    assertEquals(
        expectedApiStatus,
        configStatusManager.getAnomalyConfigStatus(requestContext, apiConfigScope));
  }

  private void upsertApiConfigStatus(AnomalyConfigStatusChange configStatus)
      throws InvalidProtocolBufferException {
    configServiceBlockingStub.upsertConfig(
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(
                AnomalyGlobalConfigServiceConstants.ANOMALY_GLOBAL_CONFIG_NAMESPACE)
            .setResourceName(
                AnomalyGlobalConfigServiceConstants.ANOMALY_GLOBAL_CONFIG_STATUS_RESOURCE_NAME)
            .setConfig(configConverter.convert(configStatus))
            .setContext(apiScope.getId())
            .build());
  }

  private void upsertServiceConfigStatus(AnomalyConfigStatusChange configStatus)
      throws InvalidProtocolBufferException {
    configServiceBlockingStub.upsertConfig(
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(
                AnomalyGlobalConfigServiceConstants.ANOMALY_GLOBAL_CONFIG_NAMESPACE)
            .setResourceName(
                AnomalyGlobalConfigServiceConstants.ANOMALY_GLOBAL_CONFIG_STATUS_RESOURCE_NAME)
            .setConfig(configConverter.convert(configStatus))
            .setContext(serviceScope.getId())
            .build());
  }

  private void upsertCustomerConfigStatus(AnomalyConfigStatusChange configStatus, String tenantId)
      throws InvalidProtocolBufferException {
    configServiceBlockingStub.upsertConfig(
        UpsertConfigRequest.newBuilder()
            .setContext(tenantId)
            .setResourceNamespace(
                AnomalyGlobalConfigServiceConstants.ANOMALY_GLOBAL_CONFIG_NAMESPACE)
            .setResourceName(
                AnomalyGlobalConfigServiceConstants.ANOMALY_GLOBAL_CONFIG_STATUS_RESOURCE_NAME)
            .setConfig(configConverter.convert(configStatus))
            .build());
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
