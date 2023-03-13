package ai.traceable.config.service.anomaly;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyCategoryConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyEventCategory;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyEventScoreCategory;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfigMap;
import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.detector.GetAllScopedAnomalyDetectionConfigsRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ObjectBolaAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigRequest;
import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DetectorConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub configServiceStub;
  private final AnomalyServiceScope serviceScope =
      AnomalyServiceScope.newBuilder().setId("service").build();
  private final AnomalyApiScope apiScope =
      AnomalyApiScope.newBuilder().setId("api").setServiceScope(serviceScope).build();
  private final AnomalyConfigScope apiConfigScope =
      AnomalyConfigScope.newBuilder().setApiScope(apiScope).build();
  private final AnomalyConfigScope customerConfigScope =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();
  private final AnomalyConfigScope serviceConfigScope =
      AnomalyConfigScope.newBuilder().setServiceScope(serviceScope).build();

  @BeforeAll
  static void init() {
    configServiceStub =
        DetectorConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testGetAndUpdateSessionDefinitionMetadataConfigs() {
    ScopedAnomalyDetectionConfig detectionConfig;
    assertThrows(
        RuntimeException.class, () -> fetchDetectorConfig(AnomalyConfigScope.newBuilder().build()));
    assertThrows(
        RuntimeException.class,
        () ->
            updateDetectorConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(AnomalyConfigScope.newBuilder().build())
                    .build()));
    detectionConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(serviceConfigScope)
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setSessionDefinitionMetadataAnomalyDetectionConfig(
                        SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                            .setObjectBola(
                                ObjectBolaAnomalyConfig.newBuilder()
                                    .setAnySourceCorrelationProbability(0.5)
                                    .setDisabledForMissingPrecedingParam(true)
                                    .build())
                            .setSubRuleConfigs(
                                AnomalySubRuleConfigMap.newBuilder()
                                    .putSubRuleConfigs(
                                        "sr2",
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("sr2")
                                            .setCategoryConfig(
                                                AnomalyCategoryConfig.newBuilder()
                                                    .setEventCategory(
                                                        AnomalyEventCategory
                                                            .ANOMALY_EVENT_CATEGORY_LATENT))
                                            .build())
                                    .build())
                            .build()))
            .build();
    updateDetectorConfig(detectionConfig);
    detectionConfig = fetchDetectorConfig(serviceConfigScope);
    SessionDefinitionMetadataAnomalyDetectionConfig sessionDefinitionConfig =
        detectionConfig.getAnomalyDetectionConfigsList().stream()
            .filter(AnomalyDetectionConfig::hasSessionDefinitionMetadataAnomalyDetectionConfig)
            .map(AnomalyDetectionConfig::getSessionDefinitionMetadataAnomalyDetectionConfig)
            .collect(Collectors.toList())
            .get(0);
    assertEquals(0.5, sessionDefinitionConfig.getObjectBola().getAnySourceCorrelationProbability());
    assertTrue(sessionDefinitionConfig.getObjectBola().getDisabledForMissingPrecedingParam());
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        sessionDefinitionConfig
            .getSubRuleConfigs()
            .getSubRuleConfigsMap()
            .get("sr2")
            .getCategoryConfig()
            .getEventCategory());
  }

  @Test
  void testGetAndUpdateModsecConfigs() {
    ScopedAnomalyDetectionConfig detectionConfig;
    assertThrows(
        RuntimeException.class, () -> fetchDetectorConfig(AnomalyConfigScope.newBuilder().build()));
    assertThrows(
        RuntimeException.class,
        () ->
            updateDetectorConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(AnomalyConfigScope.newBuilder().build())
                    .build()));

    detectionConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(customerConfigScope)
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAnomalyRule(
                                ModsecurityAnomalyRuleConfig.newBuilder()
                                    .setAnomalyRuleId("crs_913")
                                    .addSubRuleConfigs(
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("crs_913100")
                                            .setCategoryConfig(
                                                AnomalyCategoryConfig.newBuilder()
                                                    .setEventCategory(
                                                        AnomalyEventCategory
                                                            .ANOMALY_EVENT_CATEGORY_LATENT)
                                                    .setEventScoreCategory(
                                                        AnomalyEventScoreCategory
                                                            .ANOMALY_EVENT_SCORE_CATEGORY_LOW)
                                                    .build())
                                            .build())
                                    .build()))
                    .build())
            .build();

    updateDetectorConfig(detectionConfig);

    detectionConfig = fetchDetectorConfig(customerConfigScope);
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        getSubRuleEventScoreCategory(detectionConfig));

    detectionConfig = fetchDetectorConfig(serviceConfigScope);
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        getSubRuleEventScoreCategory(detectionConfig));

    detectionConfig = fetchDetectorConfig(apiConfigScope);
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        getSubRuleEventScoreCategory(detectionConfig));

    detectionConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(serviceConfigScope)
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAnomalyRule(
                                ModsecurityAnomalyRuleConfig.newBuilder()
                                    .setAnomalyRuleId("crs_913")
                                    .addSubRuleConfigs(
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("crs_913100")
                                            .setCategoryConfig(
                                                AnomalyCategoryConfig.newBuilder()
                                                    .setEventCategory(
                                                        AnomalyEventCategory
                                                            .ANOMALY_EVENT_CATEGORY_MALICIOUS)
                                                    .setEventScoreCategory(
                                                        AnomalyEventScoreCategory
                                                            .ANOMALY_EVENT_SCORE_CATEGORY_HIGH)
                                                    .build())
                                            .build())
                                    .build()))
                    .build())
            .build();

    updateDetectorConfig(detectionConfig);

    detectionConfig = fetchDetectorConfig(customerConfigScope);
    // assert customer scope config remains same
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        getSubRuleEventScoreCategory(detectionConfig));

    detectionConfig = fetchDetectorConfig(serviceConfigScope);
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_MALICIOUS,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_HIGH,
        getSubRuleEventScoreCategory(detectionConfig));

    detectionConfig = fetchDetectorConfig(apiConfigScope);
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_MALICIOUS,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_HIGH,
        getSubRuleEventScoreCategory(detectionConfig));

    detectionConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(apiConfigScope)
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAnomalyRule(
                                ModsecurityAnomalyRuleConfig.newBuilder()
                                    .setAnomalyRuleId("crs_913")
                                    .addSubRuleConfigs(
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("crs_913100")
                                            .setCategoryConfig(
                                                AnomalyCategoryConfig.newBuilder()
                                                    .setEventCategory(
                                                        AnomalyEventCategory
                                                            .ANOMALY_EVENT_CATEGORY_LATENT)
                                                    .setEventScoreCategory(
                                                        AnomalyEventScoreCategory
                                                            .ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM)
                                                    .build())
                                            .build())
                                    .build()))
                    .build())
            .build();

    updateDetectorConfig(detectionConfig);

    detectionConfig = fetchDetectorConfig(customerConfigScope);
    // assert customer scope config remains same
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        getSubRuleEventScoreCategory(detectionConfig));

    detectionConfig = fetchDetectorConfig(serviceConfigScope);
    // assert service scope config remains same
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_MALICIOUS,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_HIGH,
        getSubRuleEventScoreCategory(detectionConfig));

    detectionConfig = fetchDetectorConfig(apiConfigScope);
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM,
        getSubRuleEventScoreCategory(detectionConfig));
  }

  @Test
  void testGetAllModsecConfigs() {
    ScopedAnomalyDetectionConfig detectionConfig;
    assertThrows(
        RuntimeException.class, () -> fetchDetectorConfig(AnomalyConfigScope.newBuilder().build()));
    assertThrows(
        RuntimeException.class,
        () ->
            updateDetectorConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(AnomalyConfigScope.newBuilder().build())
                    .build()));

    detectionConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(customerConfigScope)
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAnomalyRule(
                                ModsecurityAnomalyRuleConfig.newBuilder()
                                    .setAnomalyRuleId("crs_913")
                                    .addSubRuleConfigs(
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("crs_913100")
                                            .setCategoryConfig(
                                                AnomalyCategoryConfig.newBuilder()
                                                    .setEventCategory(
                                                        AnomalyEventCategory
                                                            .ANOMALY_EVENT_CATEGORY_LATENT)
                                                    .setEventScoreCategory(
                                                        AnomalyEventScoreCategory
                                                            .ANOMALY_EVENT_SCORE_CATEGORY_LOW)
                                                    .build())
                                            .build())
                                    .build()))
                    .build())
            .build();

    updateDetectorConfig(detectionConfig);

    List<ScopedAnomalyDetectionConfig> detectionConfigs = fetchAllDetectorConfigs();
    assertEquals(1, detectionConfigs.size());

    detectionConfig = getScopedAnomalyDetectionConfig(detectionConfigs, customerConfigScope);
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        getSubRuleEventScoreCategory(detectionConfig));

    detectionConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(serviceConfigScope)
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAnomalyRule(
                                ModsecurityAnomalyRuleConfig.newBuilder()
                                    .setAnomalyRuleId("crs_913")
                                    .addSubRuleConfigs(
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("crs_913100")
                                            .setCategoryConfig(
                                                AnomalyCategoryConfig.newBuilder()
                                                    .setEventCategory(
                                                        AnomalyEventCategory
                                                            .ANOMALY_EVENT_CATEGORY_MALICIOUS)
                                                    .setEventScoreCategory(
                                                        AnomalyEventScoreCategory
                                                            .ANOMALY_EVENT_SCORE_CATEGORY_HIGH)
                                                    .build())
                                            .build())
                                    .build()))
                    .build())
            .build();

    updateDetectorConfig(detectionConfig);

    detectionConfigs = fetchAllDetectorConfigs();
    assertEquals(2, detectionConfigs.size());

    detectionConfig = getScopedAnomalyDetectionConfig(detectionConfigs, customerConfigScope);
    // assert customer scope config remains same
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        getSubRuleEventScoreCategory(detectionConfig));

    detectionConfig = getScopedAnomalyDetectionConfig(detectionConfigs, serviceConfigScope);
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_MALICIOUS,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_HIGH,
        getSubRuleEventScoreCategory(detectionConfig));

    detectionConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(apiConfigScope)
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAnomalyRule(
                                ModsecurityAnomalyRuleConfig.newBuilder()
                                    .setAnomalyRuleId("crs_913")
                                    .addSubRuleConfigs(
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("crs_913100")
                                            .setCategoryConfig(
                                                AnomalyCategoryConfig.newBuilder()
                                                    .setEventCategory(
                                                        AnomalyEventCategory
                                                            .ANOMALY_EVENT_CATEGORY_LATENT)
                                                    .setEventScoreCategory(
                                                        AnomalyEventScoreCategory
                                                            .ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM)
                                                    .build())
                                            .build())
                                    .build()))
                    .build())
            .build();

    updateDetectorConfig(detectionConfig);

    detectionConfigs = fetchAllDetectorConfigs();
    assertEquals(3, detectionConfigs.size());

    detectionConfig = getScopedAnomalyDetectionConfig(detectionConfigs, customerConfigScope);
    // assert customer scope config remains same
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        getSubRuleEventScoreCategory(detectionConfig));

    detectionConfig = getScopedAnomalyDetectionConfig(detectionConfigs, serviceConfigScope);
    // assert service scope config remains same
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_MALICIOUS,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_HIGH,
        getSubRuleEventScoreCategory(detectionConfig));

    detectionConfig = getScopedAnomalyDetectionConfig(detectionConfigs, apiConfigScope);
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        getSubRuleEventCategory(detectionConfig));
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM,
        getSubRuleEventScoreCategory(detectionConfig));
  }

  private ScopedAnomalyDetectionConfig fetchDetectorConfig(AnomalyConfigScope configScope) {
    return RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                configServiceStub
                    .getScopedAnomalyDetectionConfig(
                        GetScopedAnomalyDetectionConfigRequest.newBuilder()
                            .setConfigScope(configScope)
                            .build())
                    .getScopedAnomalyDetectionConfig());
  }

  private List<ScopedAnomalyDetectionConfig> fetchAllDetectorConfigs() {
    return RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                configServiceStub
                    .getAllScopedAnomalyDetectionConfigs(
                        GetAllScopedAnomalyDetectionConfigsRequest.newBuilder().build())
                    .getScopedAnomalyDetectionConfigsList());
  }

  private ScopedAnomalyDetectionConfig updateDetectorConfig(
      ScopedAnomalyDetectionConfig detectionConfig) {
    return RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                configServiceStub
                    .updateScopedAnomalyDetectionConfig(
                        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
                            .setScopedAnomalyDetectionConfig(detectionConfig)
                            .build())
                    .getScopedAnomalyDetectionConfig());
  }

  private AnomalyEventCategory getSubRuleEventCategory(
      ScopedAnomalyDetectionConfig detectionConfig) {
    return detectionConfig
        .getAnomalyDetectionConfigsList()
        .get(0)
        .getModsecurityAnomalyDetectionConfig()
        .getModsecAnomalyRule()
        .getSubRuleConfigsList()
        .get(0)
        .getCategoryConfig()
        .getEventCategory();
  }

  private AnomalyEventScoreCategory getSubRuleEventScoreCategory(
      ScopedAnomalyDetectionConfig detectionConfig) {
    return detectionConfig
        .getAnomalyDetectionConfigsList()
        .get(0)
        .getModsecurityAnomalyDetectionConfig()
        .getModsecAnomalyRule()
        .getSubRuleConfigsList()
        .get(0)
        .getCategoryConfig()
        .getEventScoreCategory();
  }

  private ScopedAnomalyDetectionConfig getScopedAnomalyDetectionConfig(
      List<ScopedAnomalyDetectionConfig> detectionConfigs, AnomalyConfigScope configScope) {
    for (ScopedAnomalyDetectionConfig detectionConfig : detectionConfigs) {
      if (detectionConfig.getConfigScope().equals(configScope)) {
        return detectionConfig;
      }
    }
    return null;
  }
}
