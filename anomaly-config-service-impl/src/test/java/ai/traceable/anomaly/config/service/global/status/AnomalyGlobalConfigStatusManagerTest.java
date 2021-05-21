package ai.traceable.anomaly.config.service.global.status;

import static ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConstants.ANOMALY_GLOBAL_CONFIG_NAMESPACE;
import static ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConstants.ANOMALY_GLOBAL_CONFIG_STATUS_RESOURCE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;

import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConfig;
import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConstants;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScopeType;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AnomalyGlobalConfigStatusManagerTest {

  private MockGenericConfigService mockConfigService;
  private AnomalyGlobalConfigServiceConfig config;
  private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private GlobalConfigStatusConverter configConverter;
  private RequestContext requestContext;

  private AnomalyGlobalConfigStatusManager configStatusManager;

  private final AnomalyServiceScope serviceScope =
      AnomalyServiceScope.newBuilder().setId("service").build();
  private final AnomalyApiScope apiScope =
      AnomalyApiScope.newBuilder().setId("api").setServiceScope(serviceScope).build();
  private final String tenantId = "tenant";

  private final AnomalyConfigScope customerConfigScope =
      AnomalyConfigScope.newBuilder()
          .setScopeType(AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_CUSTOMER)
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

  @BeforeEach
  public void setup() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());

    config =
        new AnomalyGlobalConfigServiceConfig(
            ConfigFactory.parseMap(Map.of("disabled", true, "internal", false)));
    configConverter = new GlobalConfigStatusConverter();
    this.configStatusManager =
        spy(
            new AnomalyGlobalConfigStatusManager(
                config, configServiceBlockingStub, configConverter));

    requestContext = RequestContext.forTenantId(tenantId);
  }

  @AfterEach
  public void teardown() {
    mockConfigService.shutdown();
  }

  @Test
  public void testGetAnomalyConfigStatus() throws InvalidProtocolBufferException {
    AnomalyConfigStatusChange configStatusChange;
    AnomalyConfigStatus expectedStatus;

    assertThrows(
        RuntimeException.class,
        () ->
            configStatusManager.getAnomalyConfigStatus(
                requestContext, AnomalyConfigScope.getDefaultInstance()));

    expectedStatus =
        AnomalyConfigStatus.newBuilder()
            .setInternal(false)
            .setDisabled(true)
            .build(); // default status
    assertEquals(
        expectedStatus,
        configStatusManager.getAnomalyConfigStatus(requestContext, customerConfigScope));

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(false).build();
    upsertCustomerConfigStatus(configStatusChange);
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
  public void testUpdateAnomalyConfigStatus() throws InvalidProtocolBufferException {
    assertThrows(
        RuntimeException.class,
        () ->
            configStatusManager.updateAnomalyConfigStatus(
                requestContext,
                AnomalyConfigScope.getDefaultInstance(),
                AnomalyConfigStatusChange.getDefaultInstance()));

    AnomalyConfigStatusChange configStatusChange;

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(true).build();
    assertEquals(
        configStatusChange,
        configStatusManager.updateAnomalyConfigStatus(
            requestContext, customerConfigScope, configStatusChange));
    AnomalyConfigStatus expectedCustomerStatus =
        AnomalyConfigStatus.newBuilder().setInternal(true).build();
    assertEquals(
        expectedCustomerStatus,
        configConverter.convert(
            fetchCustomerConfigStatus(), AnomalyConfigStatus.getDefaultInstance()));
    assertEquals(
        expectedCustomerStatus,
        configConverter.convert(
            fetchServiceConfigStatus(), AnomalyConfigStatus.getDefaultInstance()));
    assertEquals(
        expectedCustomerStatus,
        configConverter.convert(fetchApiConfigStatus(), AnomalyConfigStatus.getDefaultInstance()));

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
    assertEquals(
        configStatusChange,
        configStatusManager.updateAnomalyConfigStatus(
            requestContext, serviceConfigScope, configStatusChange));
    AnomalyConfigStatus expectedServiceStatus =
        AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build();
    assertEquals(
        expectedCustomerStatus,
        configConverter.convert(
            fetchCustomerConfigStatus(), AnomalyConfigStatus.getDefaultInstance()));
    assertEquals(
        expectedServiceStatus,
        configConverter.convert(
            fetchServiceConfigStatus(), AnomalyConfigStatus.getDefaultInstance()));
    assertEquals(
        expectedServiceStatus,
        configConverter.convert(fetchApiConfigStatus(), AnomalyConfigStatus.getDefaultInstance()));

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(false).build();
    assertEquals(
        configStatusChange,
        configStatusManager.updateAnomalyConfigStatus(
            requestContext, apiConfigScope, configStatusChange));
    AnomalyConfigStatus expectedApiStatus =
        AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build();
    assertEquals(
        expectedCustomerStatus,
        configConverter.convert(
            fetchCustomerConfigStatus(), AnomalyConfigStatus.getDefaultInstance()));
    assertEquals(
        expectedServiceStatus,
        configConverter.convert(
            fetchServiceConfigStatus(), AnomalyConfigStatus.getDefaultInstance()));
    assertEquals(
        expectedApiStatus,
        configConverter.convert(fetchApiConfigStatus(), AnomalyConfigStatus.getDefaultInstance()));
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

  private void upsertCustomerConfigStatus(AnomalyConfigStatusChange configStatus)
      throws InvalidProtocolBufferException {
    configServiceBlockingStub.upsertConfig(
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(
                AnomalyGlobalConfigServiceConstants.ANOMALY_GLOBAL_CONFIG_NAMESPACE)
            .setResourceName(
                AnomalyGlobalConfigServiceConstants.ANOMALY_GLOBAL_CONFIG_STATUS_RESOURCE_NAME)
            .setConfig(configConverter.convert(configStatus))
            .build());
  }

  private Value fetchCustomerConfigStatus() {
    return configServiceBlockingStub
        .getConfig(
            GetConfigRequest.newBuilder()
                .setResourceNamespace(ANOMALY_GLOBAL_CONFIG_NAMESPACE)
                .setResourceName(ANOMALY_GLOBAL_CONFIG_STATUS_RESOURCE_NAME)
                .build())
        .getConfig();
  }

  private Value fetchServiceConfigStatus() {
    return configServiceBlockingStub
        .getConfig(
            GetConfigRequest.newBuilder()
                .addContexts(serviceScope.getId())
                .setResourceNamespace(ANOMALY_GLOBAL_CONFIG_NAMESPACE)
                .setResourceName(ANOMALY_GLOBAL_CONFIG_STATUS_RESOURCE_NAME)
                .build())
        .getConfig();
  }

  private Value fetchApiConfigStatus() {
    return configServiceBlockingStub
        .getConfig(
            GetConfigRequest.newBuilder()
                .addAllContexts(List.of(serviceScope.getId(), apiScope.getId()))
                .setResourceNamespace(ANOMALY_GLOBAL_CONFIG_NAMESPACE)
                .setResourceName(ANOMALY_GLOBAL_CONFIG_STATUS_RESOURCE_NAME)
                .build())
        .getConfig();
  }
}
