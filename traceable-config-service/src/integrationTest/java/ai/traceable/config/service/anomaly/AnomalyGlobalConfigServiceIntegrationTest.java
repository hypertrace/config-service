package ai.traceable.config.service.anomaly;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.global.GetAllScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.global.UpdateScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.license.metering.service.api.v1.LicenseInfo;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
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
  public void test_getScopedAnomalyConfigStatus() {
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
            getScopedAnomalyConfigStatus(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build()));

    {
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder()
              .setInternal(false)
              .setDisabled(false)
              .build(); // default status
      assertEquals(
          expectedCustomerStatus,
          getScopedAnomalyConfigStatus(requestContext, customerConfigScope).getConfigStatus());
      scopedConfigs = getAllScopedAnomalyConfigStatusConfigs(requestContext);
      assertEquals(1, scopedConfigs.size());
      assertEquals(customerConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(
          AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_HIGH,
          scopedConfigs.get(0).getMinConfidenceLevel());
    }
    {
      RequestContext teamTrialRequestContext =
          RequestContext.forTenantId(tenantId + "_" + LicenseInfo.Tier.TIER_TEAM_TRIAL);
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build();
      assertEquals(
          expectedCustomerStatus,
          getScopedAnomalyConfigStatus(teamTrialRequestContext, customerConfigScope)
              .getConfigStatus());
      scopedConfigs = getAllScopedAnomalyConfigStatusConfigs(teamTrialRequestContext);
      assertEquals(1, scopedConfigs.size());
      assertEquals(customerConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(
          AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_HIGH,
          scopedConfigs.get(0).getMinConfidenceLevel());
    }
    {
      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(false).build();
      updateScopedAnomalyConfigStatus(
          requestContext, customerConfigScope, configStatusChange, Optional.empty());
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build();
      assertEquals(
          expectedCustomerStatus,
          getScopedAnomalyConfigStatus(requestContext, customerConfigScope).getConfigStatus());
      assertEquals(
          expectedCustomerStatus,
          getScopedAnomalyConfigStatus(requestContext, serviceConfigScope).getConfigStatus());
      assertEquals(
          expectedCustomerStatus,
          getScopedAnomalyConfigStatus(requestContext, apiConfigScope).getConfigStatus());
      scopedConfigs = getAllScopedAnomalyConfigStatusConfigs(requestContext);
      assertEquals(1, scopedConfigs.size());
      assertEquals(customerConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(
          AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_HIGH,
          scopedConfigs.get(0).getMinConfidenceLevel());
    }
    {
      configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(true).build();
      updateScopedAnomalyConfigStatus(
          requestContext,
          apiConfigScope,
          configStatusChange,
          Optional.of(AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_MEDIUM));
      expectedApiStatus =
          AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(false).build();
      assertEquals(
          expectedApiStatus,
          getScopedAnomalyConfigStatus(requestContext, apiConfigScope).getConfigStatus());
      // customer config stays unchanged..
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build();
      assertEquals(
          expectedCustomerStatus,
          getScopedAnomalyConfigStatus(requestContext, customerConfigScope).getConfigStatus());
      assertEquals(
          expectedCustomerStatus,
          getScopedAnomalyConfigStatus(requestContext, serviceConfigScope).getConfigStatus());
      scopedConfigs = getAllScopedAnomalyConfigStatusConfigs(requestContext);
      assertEquals(2, scopedConfigs.size());
      assertEquals(apiConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedApiStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(
          AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_MEDIUM,
          scopedConfigs.get(0).getMinConfidenceLevel());
      assertEquals(customerConfigScope, scopedConfigs.get(1).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(1).getConfigStatus());
      assertEquals(
          AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_HIGH,
          scopedConfigs.get(1).getMinConfidenceLevel());
    }
    {
      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
      updateScopedAnomalyConfigStatus(
          requestContext, serviceConfigScope, configStatusChange, Optional.empty());
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build();
      expectedServiceStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build();
      expectedApiStatus =
          AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build();
      assertEquals(
          expectedCustomerStatus,
          getScopedAnomalyConfigStatus(requestContext, customerConfigScope).getConfigStatus());
      assertEquals(
          expectedServiceStatus,
          getScopedAnomalyConfigStatus(requestContext, serviceConfigScope).getConfigStatus());
      assertEquals(
          expectedApiStatus,
          getScopedAnomalyConfigStatus(requestContext, apiConfigScope).getConfigStatus());
      scopedConfigs = getAllScopedAnomalyConfigStatusConfigs(requestContext);
      assertEquals(3, scopedConfigs.size());
      assertEquals(serviceConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedServiceStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(
          AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_HIGH,
          scopedConfigs.get(0).getMinConfidenceLevel());
      assertEquals(apiConfigScope, scopedConfigs.get(1).getConfigScope());
      assertEquals(expectedApiStatus, scopedConfigs.get(1).getConfigStatus());
      assertEquals(
          AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_MEDIUM,
          scopedConfigs.get(1).getMinConfidenceLevel());
      assertEquals(customerConfigScope, scopedConfigs.get(2).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(2).getConfigStatus());
      assertEquals(
          AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_HIGH,
          scopedConfigs.get(2).getMinConfidenceLevel());
    }
  }

  @Test
  public void test_updateScopedAnomalyConfigStatus() {
    RequestContext requestContext = RequestContext.forTenantId("update_tenant");

    assertThrows(
        RuntimeException.class,
        () ->
            updateScopedAnomalyConfigStatus(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build(),
                AnomalyConfigStatusChange.getDefaultInstance(),
                Optional.empty()));

    AnomalyConfigStatusChange configStatusChange;
    AnomalyConfigStatus expectedCustomerStatus;
    {
      configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(true).build();
      assertEquals(
          configStatusChange,
          updateScopedAnomalyConfigStatus(
              requestContext, customerConfigScope, configStatusChange, Optional.empty()));
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(false).build();
      assertEquals(
          expectedCustomerStatus,
          getScopedAnomalyConfigStatus(requestContext, customerConfigScope).getConfigStatus());
      assertEquals(
          expectedCustomerStatus,
          getScopedAnomalyConfigStatus(requestContext, serviceConfigScope).getConfigStatus());
      assertEquals(
          expectedCustomerStatus,
          getScopedAnomalyConfigStatus(requestContext, apiConfigScope).getConfigStatus());

      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
      assertEquals(
          AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build(),
          updateScopedAnomalyConfigStatus(
              requestContext, customerConfigScope, configStatusChange, Optional.empty()));
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setDisabled(true).setInternal(true).build();
      assertEquals(
          expectedCustomerStatus,
          getScopedAnomalyConfigStatus(requestContext, customerConfigScope).getConfigStatus());
    }

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
    assertEquals(
        configStatusChange,
        updateScopedAnomalyConfigStatus(
            requestContext, serviceConfigScope, configStatusChange, Optional.empty()));
    AnomalyConfigStatus expectedServiceStatus =
        AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build();
    assertEquals(
        expectedCustomerStatus,
        getScopedAnomalyConfigStatus(requestContext, customerConfigScope).getConfigStatus());
    assertEquals(
        expectedServiceStatus,
        getScopedAnomalyConfigStatus(requestContext, serviceConfigScope).getConfigStatus());
    assertEquals(
        expectedServiceStatus,
        getScopedAnomalyConfigStatus(requestContext, apiConfigScope).getConfigStatus());

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(false).build();
    assertEquals(
        configStatusChange,
        updateScopedAnomalyConfigStatus(
            requestContext, apiConfigScope, configStatusChange, Optional.empty()));
    AnomalyConfigStatus expectedApiStatus =
        AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build();
    assertEquals(
        expectedCustomerStatus,
        getScopedAnomalyConfigStatus(requestContext, customerConfigScope).getConfigStatus());
    assertEquals(
        expectedServiceStatus,
        getScopedAnomalyConfigStatus(requestContext, serviceConfigScope).getConfigStatus());
    assertEquals(
        expectedApiStatus,
        getScopedAnomalyConfigStatus(requestContext, apiConfigScope).getConfigStatus());
  }

  private ScopedAnomalyConfigStatus getScopedAnomalyConfigStatus(
      RequestContext requestContext, AnomalyConfigScope configScope) {
    return requestContext.call(
        () ->
            configServiceStub
                .getScopedAnomalyGlobalConfigStatus(
                    GetScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                        .setConfigScope(configScope)
                        .build())
                .getScopedConfig());
  }

  private List<ScopedAnomalyConfigStatus> getAllScopedAnomalyConfigStatusConfigs(
      RequestContext requestContext) {
    return requestContext.call(
        () ->
            configServiceStub
                .getAllScopedAnomalyGlobalConfigStatus(
                    GetAllScopedAnomalyGlobalConfigStatusRequest.getDefaultInstance())
                .getScopedConfigsList());
  }

  private AnomalyConfigStatusChange updateScopedAnomalyConfigStatus(
      RequestContext requestContext,
      AnomalyConfigScope scope,
      AnomalyConfigStatusChange status,
      Optional<AnomalyConfidenceLevel> confidenceLevel) {
    ScopedAnomalyConfigStatusChange.Builder scopedConfigBuilder =
        ScopedAnomalyConfigStatusChange.newBuilder().setConfigScope(scope).setConfigStatus(status);
    confidenceLevel.ifPresent(scopedConfigBuilder::setMinConfidenceLevel);
    ScopedAnomalyConfigStatusChange updatedConfig =
        requestContext
            .call(
                () ->
                    configServiceStub.updateScopedAnomalyGlobalConfigStatus(
                        UpdateScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                            .setScopedConfig(scopedConfigBuilder)
                            .build()))
            .getScopedConfig();
    assertEquals(scope, updatedConfig.getConfigScope());
    return updatedConfig.getConfigStatus();
  }
}
