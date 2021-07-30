package ai.traceable.config.service.anomaly;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusRequest;
import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.license.metering.service.api.v1.LicenseInfo;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AnomalyGlobalConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {

  private static AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub
      configServiceStub;

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
  static void init() {
    configServiceStub =
        AnomalyGlobalConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  public void testGetGlobalConfigStatus() {
    AnomalyConfigStatusChange configStatusChange;
    AnomalyConfigStatus expectedStatus;

    assertThrows(
        RuntimeException.class,
        () ->
            fetchGlobalConfigStatus(
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build()));

    assertEquals(
        AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build(),
        fetchGlobalConfigStatus(customerConfigScope, "tenant_" + LicenseInfo.Tier.TIER_TEAM_TRIAL));

    expectedStatus =
        AnomalyConfigStatus.newBuilder()
            .setInternal(false)
            .setDisabled(false)
            .build(); // default status
    assertEquals(expectedStatus, fetchGlobalConfigStatus(customerConfigScope));

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
    updateGlobalConfigStatus(customerConfigScope, configStatusChange);
    expectedStatus = AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build();
    assertEquals(expectedStatus, fetchGlobalConfigStatus(customerConfigScope));
    assertEquals(expectedStatus, fetchGlobalConfigStatus(serviceConfigScope));
    assertEquals(expectedStatus, fetchGlobalConfigStatus(apiConfigScope));

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(true).build();
    updateGlobalConfigStatus(apiConfigScope, configStatusChange);
    expectedStatus = AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build();
    assertEquals(expectedStatus, fetchGlobalConfigStatus(apiConfigScope));
    // customer config stays unchanged..
    expectedStatus = AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build();
    assertEquals(expectedStatus, fetchGlobalConfigStatus(customerConfigScope));
    assertEquals(expectedStatus, fetchGlobalConfigStatus(serviceConfigScope));

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(false).build();
    updateGlobalConfigStatus(serviceConfigScope, configStatusChange);
    assertEquals(
        AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build(),
        fetchGlobalConfigStatus(customerConfigScope));
    assertEquals(
        AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build(),
        fetchGlobalConfigStatus(serviceConfigScope));
    assertEquals(
        AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(false).build(),
        fetchGlobalConfigStatus(apiConfigScope));
  }

  @Test
  public void testUpdateGlobalConfigStatus() {
    String tenantId = TENANT_ID;
    assertThrows(
        RuntimeException.class,
        () ->
            updateGlobalConfigStatus(
                AnomalyConfigScope.getDefaultInstance(),
                AnomalyConfigStatusChange.getDefaultInstance()));

    AnomalyConfigStatusChange configStatusChange;

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(true).build();
    assertEquals(
        configStatusChange, updateGlobalConfigStatus(customerConfigScope, configStatusChange));
    AnomalyConfigStatus expectedCustomerStatus =
        AnomalyConfigStatus.newBuilder().setDisabled(false).setInternal(true).build();
    assertEquals(expectedCustomerStatus, fetchGlobalConfigStatus(customerConfigScope));
    assertEquals(expectedCustomerStatus, fetchGlobalConfigStatus(serviceConfigScope));
    assertEquals(expectedCustomerStatus, fetchGlobalConfigStatus(apiConfigScope));

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
    assertEquals(
        configStatusChange, updateGlobalConfigStatus(serviceConfigScope, configStatusChange));
    AnomalyConfigStatus expectedServiceStatus =
        AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build();
    assertEquals(expectedCustomerStatus, fetchGlobalConfigStatus(customerConfigScope));
    assertEquals(expectedServiceStatus, fetchGlobalConfigStatus(serviceConfigScope));
    assertEquals(expectedServiceStatus, fetchGlobalConfigStatus(apiConfigScope));

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(false).build();
    assertEquals(configStatusChange, updateGlobalConfigStatus(apiConfigScope, configStatusChange));
    AnomalyConfigStatus expectedApiStatus =
        AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build();
    assertEquals(expectedCustomerStatus, fetchGlobalConfigStatus(customerConfigScope));
    assertEquals(expectedServiceStatus, fetchGlobalConfigStatus(serviceConfigScope));
    assertEquals(expectedApiStatus, fetchGlobalConfigStatus(apiConfigScope));
  }

  private AnomalyConfigStatus fetchGlobalConfigStatus(AnomalyConfigScope configScope) {
    return fetchGlobalConfigStatus(configScope, TENANT_ID);
  }

  private AnomalyConfigStatus fetchGlobalConfigStatus(
      AnomalyConfigScope configScope, String tenantId) {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        tenantId,
        () ->
            configServiceStub
                .getAnomalyGlobalConfigStatus(
                    GetAnomalyGlobalConfigStatusRequest.newBuilder()
                        .setConfigScope(configScope)
                        .build())
                .getConfigStatus());
  }

  private AnomalyConfigStatusChange updateGlobalConfigStatus(
      AnomalyConfigScope configScope, AnomalyConfigStatusChange configStatus) {
    return GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                configServiceStub.updateAnomalyGlobalConfigStatus(
                    UpdateAnomalyGlobalConfigStatusRequest.newBuilder()
                        .setConfigScope(configScope)
                        .setConfigStatus(configStatus)
                        .build()))
        .getConfigStatus();
  }
}
