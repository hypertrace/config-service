package ai.traceable.anomaly.config.service.aggregator;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.aggregator.config.AggregationConfigServiceConfig;
import ai.traceable.anomaly.config.service.aggregator.handler.AnomalyAggregationConfigHandler;
import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.aggregator.AggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationFamilyConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationGlobalConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.ScopedAnomalyEventAggregationConfig;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AggregationConfigManagerTest {
  private static Server mockServer;
  private static MockGenericConfigService mockConfigService;

  private final AnomalyAggregationConfigHandler anomalyAggregationConfigHandler =
      new AnomalyAggregationConfigHandler();
  private AggregationConfigManager configManager;

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

  @BeforeEach
  public void setup() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    mockServer = InProcessServerBuilder.forName(serverName).build().start();
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    AggregationConfigServiceConfig aggregationConfigServiceConfig =
        mock(AggregationConfigServiceConfig.class);
    when(aggregationConfigServiceConfig.getDefaultModsecAggregationConfig())
        .thenReturn(Optional.empty());
    when(aggregationConfigServiceConfig.getDefaultGlobalAggregationConfig())
        .thenReturn(Optional.empty());
    this.configManager =
        spy(
            new AggregationConfigManagerImpl(
                configServiceBlockingStub,
                anomalyAggregationConfigHandler,
                new AnomalyConfigScopeUtils(),
                aggregationConfigServiceConfig,
                mock(ConfigChangeEventGenerator.class)));
  }

  @Test
  void testResolvedAggregationConfigs() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ScopedAnomalyEventAggregationConfig apiScopedAnomalyEventAggregationConfig =
        getApiScopedAnomalyEventAggregationConfig();
    ScopedAnomalyEventAggregationConfig serviceScopedAnomalyEventAggregationConfig =
        getServiceScopedAnomalyEventAggregationConfig();
    ScopedAnomalyEventAggregationConfig customerScopedAnomalyEventAggregationConfig =
        getCustomerScopedAnomalyEventAggregationConfig();

    assertThrows(
        RuntimeException.class,
        () ->
            configManager.getScopedAnomalyAggregationConfig(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build()));

    List<ScopedAnomalyEventAggregationConfig> getAllScopedAnomalyEventAggregationConfigs =
        configManager.getAllScopedAnomalyEventAggregationConfigs(requestContext);

    Assertions.assertEquals(0, getAllScopedAnomalyEventAggregationConfigs.size());

    ScopedAnomalyEventAggregationConfig getScopedAnomalyAggregationConfig =
        configManager.getScopedAnomalyAggregationConfig(requestContext, apiConfigScope);
    Assertions.assertEquals(
        EventAggregationConfig.getDefaultInstance(),
        getScopedAnomalyAggregationConfig.getEventAggregationConfig());

    updateScopedAnomalyAggregationConfig(
        requestContext, serviceScopedAnomalyEventAggregationConfig);

    getAllScopedAnomalyEventAggregationConfigs =
        configManager.getAllScopedAnomalyEventAggregationConfigs(requestContext);
    Assertions.assertEquals(1, getAllScopedAnomalyEventAggregationConfigs.size());

    getScopedAnomalyAggregationConfig =
        configManager.getScopedAnomalyAggregationConfig(requestContext, apiConfigScope);
    verifyScopedAnomalyEventAggregationConfigs(
        getServiceScopedAnomalyEventAggregationConfig(), getScopedAnomalyAggregationConfig);

    getScopedAnomalyAggregationConfig =
        configManager.getScopedAnomalyAggregationConfig(requestContext, serviceConfigScope);
    verifyScopedAnomalyEventAggregationConfigs(
        getServiceScopedAnomalyEventAggregationConfig(), getScopedAnomalyAggregationConfig);

    getScopedAnomalyAggregationConfig =
        configManager.getScopedAnomalyAggregationConfig(requestContext, customerConfigScope);
    Assertions.assertEquals(
        EventAggregationConfig.getDefaultInstance(),
        getScopedAnomalyAggregationConfig.getEventAggregationConfig());

    updateScopedAnomalyAggregationConfig(requestContext, apiScopedAnomalyEventAggregationConfig);
    getAllScopedAnomalyEventAggregationConfigs =
        configManager.getAllScopedAnomalyEventAggregationConfigs(requestContext);
    Assertions.assertEquals(2, getAllScopedAnomalyEventAggregationConfigs.size());
    verifyScopedAnomalyConfigsWithServiceAndApiScopes(getAllScopedAnomalyEventAggregationConfigs);
    getScopedAnomalyAggregationConfig =
        configManager.getScopedAnomalyAggregationConfig(requestContext, apiConfigScope);
    verifyScopedAnomalyEventAggregationConfigs(
        getApiScopedAnomalyEventAggregationConfigMergedWithService(),
        getScopedAnomalyAggregationConfig);

    updateScopedAnomalyAggregationConfig(
        requestContext, customerScopedAnomalyEventAggregationConfig);
    getAllScopedAnomalyEventAggregationConfigs =
        configManager.getAllScopedAnomalyEventAggregationConfigs(requestContext);
    Assertions.assertEquals(3, getAllScopedAnomalyEventAggregationConfigs.size());
    verifyScopedAnomalyConfigsWithServiceApiAndCustomerScopes(
        getAllScopedAnomalyEventAggregationConfigs);

    deleteScopedAnomalyAggregationConfig(
        requestContext,
        ScopedAnomalyEventAggregationConfig.newBuilder()
            .setConfigScope(customerConfigScope)
            .build());
    getAllScopedAnomalyEventAggregationConfigs =
        configManager.getAllScopedAnomalyEventAggregationConfigs(requestContext);
    Assertions.assertEquals(2, getAllScopedAnomalyEventAggregationConfigs.size());
    verifyScopedAnomalyConfigsWithServiceAndApiScopes(getAllScopedAnomalyEventAggregationConfigs);
  }

  @Test
  public void testUnresolvedConfigs() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ScopedAnomalyEventAggregationConfig apiScopedAnomalyEventAggregationConfig =
        getApiScopedAnomalyEventAggregationConfig();
    ScopedAnomalyEventAggregationConfig serviceScopedAnomalyEventAggregationConfig =
        getServiceScopedAnomalyEventAggregationConfig();
    List<ScopedAnomalyEventAggregationConfig> getAllUnresolvedAnomalyEventAggregationConfigs =
        configManager.getAllUnresolvedScopedAnomalyEventAggregationConfigs(requestContext);

    Assertions.assertEquals(0, getAllUnresolvedAnomalyEventAggregationConfigs.size());

    ScopedAnomalyEventAggregationConfig getUnresolvedScopedAnomalyAggregationConfig =
        configManager.getUnresolvedScopedAnomalyEventAggregationConfig(
            requestContext, apiConfigScope);
    Assertions.assertEquals(
        EventAggregationConfig.getDefaultInstance(),
        getUnresolvedScopedAnomalyAggregationConfig.getEventAggregationConfig());

    updateScopedAnomalyAggregationConfig(
        requestContext, serviceScopedAnomalyEventAggregationConfig);

    getAllUnresolvedAnomalyEventAggregationConfigs =
        configManager.getAllUnresolvedScopedAnomalyEventAggregationConfigs(requestContext);
    Assertions.assertEquals(1, getAllUnresolvedAnomalyEventAggregationConfigs.size());

    getUnresolvedScopedAnomalyAggregationConfig =
        configManager.getUnresolvedScopedAnomalyEventAggregationConfig(
            requestContext, apiConfigScope);
    Assertions.assertEquals(
        ScopedAnomalyEventAggregationConfig.newBuilder().setConfigScope(apiConfigScope).build(),
        getUnresolvedScopedAnomalyAggregationConfig);

    getUnresolvedScopedAnomalyAggregationConfig =
        configManager.getUnresolvedScopedAnomalyEventAggregationConfig(
            requestContext, serviceConfigScope);
    Assertions.assertEquals(
        getServiceScopedAnomalyEventAggregationConfig(),
        getUnresolvedScopedAnomalyAggregationConfig);

    updateScopedAnomalyAggregationConfig(requestContext, apiScopedAnomalyEventAggregationConfig);
    getAllUnresolvedAnomalyEventAggregationConfigs =
        configManager.getAllUnresolvedScopedAnomalyEventAggregationConfigs(requestContext);
    Assertions.assertEquals(2, getAllUnresolvedAnomalyEventAggregationConfigs.size());
    verifyUnresolvedScopedAnomalyConfigsWithServiceAndApiScopes(
        getAllUnresolvedAnomalyEventAggregationConfigs);

    getUnresolvedScopedAnomalyAggregationConfig =
        configManager.getUnresolvedScopedAnomalyEventAggregationConfig(
            requestContext, apiConfigScope);
    verifyScopedAnomalyEventAggregationConfigs(
        getApiScopedAnomalyEventAggregationConfig(), getUnresolvedScopedAnomalyAggregationConfig);
  }

  private void updateScopedAnomalyAggregationConfig(
      RequestContext requestContext,
      ScopedAnomalyEventAggregationConfig scopedAnomalyEventAggregationConfig) {
    requestContext.run(
        () ->
            configManager.updateScopedAnomalyEventAggregationConfig(
                requestContext, scopedAnomalyEventAggregationConfig));
  }

  private void deleteScopedAnomalyAggregationConfig(
      RequestContext requestContext,
      ScopedAnomalyEventAggregationConfig scopedAnomalyEventAggregationConfig) {
    requestContext.run(
        () ->
            configManager.deleteScopedAnomalyEventAggregationConfig(
                requestContext, scopedAnomalyEventAggregationConfig.getConfigScope()));
  }

  private void verifyScopedAnomalyConfigsWithServiceAndApiScopes(
      List<ScopedAnomalyEventAggregationConfig> getAllScopedAnomalyEventAggregationConfigs) {
    for (ScopedAnomalyEventAggregationConfig scopedAnomalyEventAggregationConfig :
        getAllScopedAnomalyEventAggregationConfigs) {
      if (scopedAnomalyEventAggregationConfig.getConfigScope().equals(apiConfigScope)) {
        verifyScopedAnomalyEventAggregationConfigs(
            getApiScopedAnomalyEventAggregationConfigMergedWithService(),
            scopedAnomalyEventAggregationConfig);
      } else if (scopedAnomalyEventAggregationConfig.getConfigScope().equals(serviceConfigScope)) {
        verifyScopedAnomalyEventAggregationConfigs(
            getServiceScopedAnomalyEventAggregationConfig(), scopedAnomalyEventAggregationConfig);
      }
    }
  }

  private void verifyUnresolvedScopedAnomalyConfigsWithServiceAndApiScopes(
      List<ScopedAnomalyEventAggregationConfig> getAllScopedAnomalyEventAggregationConfigs) {
    for (ScopedAnomalyEventAggregationConfig scopedAnomalyEventAggregationConfig :
        getAllScopedAnomalyEventAggregationConfigs) {
      if (scopedAnomalyEventAggregationConfig.getConfigScope().equals(apiConfigScope)) {
        verifyScopedAnomalyEventAggregationConfigs(
            getApiScopedAnomalyEventAggregationConfig(), scopedAnomalyEventAggregationConfig);
      } else if (scopedAnomalyEventAggregationConfig.getConfigScope().equals(serviceConfigScope)) {
        verifyScopedAnomalyEventAggregationConfigs(
            getServiceScopedAnomalyEventAggregationConfig(), scopedAnomalyEventAggregationConfig);
      }
    }
  }

  private void verifyScopedAnomalyConfigsWithServiceApiAndCustomerScopes(
      List<ScopedAnomalyEventAggregationConfig> getAllScopedAnomalyEventAggregationConfigs) {
    for (ScopedAnomalyEventAggregationConfig scopedAnomalyEventAggregationConfig :
        getAllScopedAnomalyEventAggregationConfigs) {
      if (scopedAnomalyEventAggregationConfig.getConfigScope().equals(apiConfigScope)) {
        verifyScopedAnomalyEventAggregationConfigs(
            getApiScopedAnomalyEventAggregationConfigMergedWithServiceAndCustomer(),
            scopedAnomalyEventAggregationConfig);
      } else if (scopedAnomalyEventAggregationConfig.getConfigScope().equals(serviceConfigScope)) {
        verifyScopedAnomalyEventAggregationConfigs(
            getServiceScopedAnomalyEventAggregationConfigMergedWithCustomer(),
            scopedAnomalyEventAggregationConfig);
      } else if (scopedAnomalyEventAggregationConfig.getConfigScope().equals(customerConfigScope)) {
        verifyScopedAnomalyEventAggregationConfigs(
            getCustomerScopedAnomalyEventAggregationConfig(), scopedAnomalyEventAggregationConfig);
      }
    }
  }

  private void verifyScopedAnomalyEventAggregationConfigs(
      ScopedAnomalyEventAggregationConfig expectedScopedAnomalyEventAggregationConfig,
      ScopedAnomalyEventAggregationConfig actualScopedAnomalyEventAggregationConfig) {
    List<EventAggregationFamilyConfig> expectedFamilyConfigs =
        expectedScopedAnomalyEventAggregationConfig
            .getEventAggregationConfig()
            .getFamilyConfigsList();
    List<EventAggregationFamilyConfig> actualFamilyConfigs =
        actualScopedAnomalyEventAggregationConfig
            .getEventAggregationConfig()
            .getFamilyConfigsList();
    assertTrue(
        expectedFamilyConfigs.size() == actualFamilyConfigs.size()
            && expectedFamilyConfigs.containsAll(actualFamilyConfigs)
            && actualFamilyConfigs.containsAll(expectedFamilyConfigs));
    Assertions.assertEquals(
        expectedScopedAnomalyEventAggregationConfig.getEventAggregationConfig().getGlobalConfig(),
        actualScopedAnomalyEventAggregationConfig.getEventAggregationConfig().getGlobalConfig());
  }

  @AfterEach
  public void teardown() {
    mockConfigService.shutdown();
    mockServer.shutdown();
  }

  private ScopedAnomalyEventAggregationConfig getApiScopedAnomalyEventAggregationConfig() {
    // Only global and api def configs are present.
    return ScopedAnomalyEventAggregationConfig.newBuilder()
        .setConfigScope(apiConfigScope)
        .setEventAggregationConfig(
            EventAggregationConfig.newBuilder()
                .setGlobalConfig(getApiScopedGlobalConfig())
                .addFamilyConfigs(getApiScopedApiDefConfig())
                .build())
        .build();
  }

  private ScopedAnomalyEventAggregationConfig
      getApiScopedAnomalyEventAggregationConfigMergedWithService() {
    return ScopedAnomalyEventAggregationConfig.newBuilder()
        .setConfigScope(apiConfigScope)
        .setEventAggregationConfig(
            EventAggregationConfig.newBuilder()
                .setGlobalConfig(getApiScopedGlobalConfig())
                .addFamilyConfigs(getApiScopedApiDefConfig())
                .addFamilyConfigs(getServiceScopedModsecConfig())
                .build())
        .build();
  }

  private ScopedAnomalyEventAggregationConfig
      getApiScopedAnomalyEventAggregationConfigMergedWithServiceAndCustomer() {
    return ScopedAnomalyEventAggregationConfig.newBuilder()
        .setConfigScope(apiConfigScope)
        .setEventAggregationConfig(
            EventAggregationConfig.newBuilder()
                .setGlobalConfig(getApiScopedGlobalConfig())
                .addFamilyConfigs(getApiScopedApiDefConfig())
                .addFamilyConfigs(getServiceScopedModsecConfig())
                .addFamilyConfigs(getCustomerScopedSessionConfig())
                .build())
        .build();
  }

  private ScopedAnomalyEventAggregationConfig getServiceScopedAnomalyEventAggregationConfig() {
    // Only global, apiDef and modsec configs are present.
    return ScopedAnomalyEventAggregationConfig.newBuilder()
        .setConfigScope(serviceConfigScope)
        .setEventAggregationConfig(
            EventAggregationConfig.newBuilder()
                .setGlobalConfig(getServiceScopedGlobalConfig())
                .addFamilyConfigs(getServiceScopedApiDefConfig())
                .addFamilyConfigs(getServiceScopedModsecConfig())
                .build())
        .build();
  }

  private ScopedAnomalyEventAggregationConfig
      getServiceScopedAnomalyEventAggregationConfigMergedWithCustomer() {
    return ScopedAnomalyEventAggregationConfig.newBuilder()
        .setConfigScope(serviceConfigScope)
        .setEventAggregationConfig(
            EventAggregationConfig.newBuilder()
                .setGlobalConfig(getServiceScopedGlobalConfig())
                .addFamilyConfigs(getServiceScopedApiDefConfig())
                .addFamilyConfigs(getServiceScopedModsecConfig())
                .addFamilyConfigs(getCustomerScopedSessionConfig())
                .build())
        .build();
  }

  private ScopedAnomalyEventAggregationConfig getCustomerScopedAnomalyEventAggregationConfig() {
    // Only global, apiDef and modsec configs are present.
    return ScopedAnomalyEventAggregationConfig.newBuilder()
        .setConfigScope(customerConfigScope)
        .setEventAggregationConfig(
            EventAggregationConfig.newBuilder()
                .setGlobalConfig(getCustomerScopedGlobalConfig())
                .addFamilyConfigs(getCustomerScopedModsecConfig())
                .addFamilyConfigs(getCustomerScopedSessionConfig())
                .build())
        .build();
  }

  private EventAggregationGlobalConfig getApiScopedGlobalConfig() {
    return EventAggregationGlobalConfig.newBuilder()
        .setParamNameMaxDuration("1d")
        .setParamValueMaxDuration("1d")
        .build();
  }

  private EventAggregationGlobalConfig getServiceScopedGlobalConfig() {
    return EventAggregationGlobalConfig.newBuilder()
        .setParamNameMaxDuration("2d")
        .setParamValueMaxDuration("2d")
        .build();
  }

  private EventAggregationGlobalConfig getCustomerScopedGlobalConfig() {
    return EventAggregationGlobalConfig.newBuilder()
        .setParamNameMaxDuration("3d")
        .setParamValueMaxDuration("3d")
        .build();
  }

  private EventAggregationFamilyConfig getApiScopedApiDefConfig() {
    return EventAggregationFamilyConfig.newBuilder()
        .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF)
        .setAggregationConfig(
            AggregationConfig.newBuilder()
                .setMaxEventsWithScorePerParamValue(10)
                .setMaxEventsWithoutScorePerParamValue(10)
                .setMaxUsersPerParam(10)
                .build())
        .build();
  }

  private EventAggregationFamilyConfig getServiceScopedApiDefConfig() {
    return EventAggregationFamilyConfig.newBuilder()
        .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF)
        .setAggregationConfig(
            AggregationConfig.newBuilder()
                .setMaxEventsWithScorePerParamValue(20)
                .setMaxEventsWithoutScorePerParamValue(20)
                .setMaxUsersPerParam(20)
                .build())
        .build();
  }

  private EventAggregationFamilyConfig getServiceScopedModsecConfig() {
    return EventAggregationFamilyConfig.newBuilder()
        .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
        .setAggregationConfig(
            AggregationConfig.newBuilder()
                .setMaxEventsWithScorePerParamValue(20)
                .setMaxEventsWithoutScorePerParamValue(20)
                .setMaxUsersPerParam(20)
                .build())
        .build();
  }

  private EventAggregationFamilyConfig getCustomerScopedSessionConfig() {
    return EventAggregationFamilyConfig.newBuilder()
        .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION)
        .setAggregationConfig(
            AggregationConfig.newBuilder()
                .setMaxEventsWithScorePerParamValue(30)
                .setMaxEventsWithoutScorePerParamValue(30)
                .setMaxUsersPerParam(30)
                .build())
        .build();
  }

  private EventAggregationFamilyConfig getCustomerScopedModsecConfig() {
    return EventAggregationFamilyConfig.newBuilder()
        .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
        .setAggregationConfig(
            AggregationConfig.newBuilder()
                .setMaxEventsWithScorePerParamValue(30)
                .setMaxEventsWithoutScorePerParamValue(30)
                .setMaxUsersPerParam(30)
                .build())
        .build();
  }
}
