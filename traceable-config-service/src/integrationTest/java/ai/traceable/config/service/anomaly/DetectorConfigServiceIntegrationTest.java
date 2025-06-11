package ai.traceable.config.service.anomaly;

import static ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_VOLUMETRIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyCategoryConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyEventCategory;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyEventScoreCategory;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfigMap;
import ai.traceable.anomaly.config.service.v1.detector.ApiCallSpikeAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.detector.GetAllGlobalResolvedScopedAnomalyDetectionConfigsRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetAllScopedAnomalyDetectionConfigsRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.GetGlobalResolvedScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ObjectBolaAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.VolumetricAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.global.UpdateScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DetectorConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub configServiceStub;

  private static AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub
      globalConfigServiceStub;
  private final AnomalyServiceScope serviceScope =
      AnomalyServiceScope.newBuilder().setId("service").build();
  private final AnomalyApiScope apiScope =
      AnomalyApiScope.newBuilder().setId("api").setServiceScope(serviceScope).build();

  private final AnomalyEnvironmentScope environmentScope =
      AnomalyEnvironmentScope.newBuilder().setEnvironmentId("env").build();
  private final AnomalyConfigScope apiConfigScope =
      AnomalyConfigScope.newBuilder().setApiScope(apiScope).build();
  private final AnomalyConfigScope customerConfigScope =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();
  private final AnomalyConfigScope serviceConfigScope =
      AnomalyConfigScope.newBuilder().setServiceScope(serviceScope).build();

  private final AnomalyConfigScope environmentConfigScope =
      AnomalyConfigScope.newBuilder().setEnvironmentScope(environmentScope).build();

  @BeforeAll
  static void init() {
    configServiceStub =
        DetectorConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    globalConfigServiceStub =
        AnomalyGlobalConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testGetAndUpdateSessionDefinitionMetadataConfigs() {
    ScopedAnomalyDetectionConfig detectionConfig;
    assertThrows(
        RuntimeException.class, () -> fetchDetectorConfig(AnomalyConfigScope.newBuilder().build()));
    assertThrows(
        Exception.class,
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
        Exception.class,
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
        Exception.class,
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

  @Test
  void testGetGlobalResolvedScopedAnomalyDetectionConfig() {
    ScopedAnomalyDetectionConfig detectionConfig1, detectionConfig2, detectionConfig3;
    assertThrows(
        RuntimeException.class,
        () -> fetchGlobalResolvedDetectorConfig(AnomalyConfigScope.newBuilder().build()));
    assertThrows(
        Exception.class,
        () ->
            updateDetectorConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(AnomalyConfigScope.newBuilder().build())
                    .build()));

    {
      detectionConfig1 =
          ScopedAnomalyDetectionConfig.newBuilder()
              .setConfigScope(customerConfigScope)
              .addAnomalyDetectionConfigs(
                  AnomalyDetectionConfig.newBuilder()
                      .setConfigStatus(
                          AnomalyConfigStatusChange.newBuilder()
                              .setDisabled(false)
                              .setInternal(true)
                              .build())
                      .setVolumetricAnomalyDetectionConfig(
                          VolumetricAnomalyDetectionConfig.newBuilder()
                              .setApiCallSpike(ApiCallSpikeAnomalyConfig.newBuilder())
                              .build()))
              .build();

      updateDetectorConfig(detectionConfig1);

      updateScopedAnomalyConfigStatus(
          RequestContext.forTenantId(TENANT_ID),
          customerConfigScope,
          AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build(),
          Optional.empty());

      detectionConfig1 = fetchGlobalResolvedDetectorConfig(customerConfigScope);

      assertEquals(
          AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build(),
          detectionConfig1.getAnomalyDetectionConfigsList().get(0).getConfigStatus());
    }
    {
      detectionConfig2 =
          ScopedAnomalyDetectionConfig.newBuilder()
              .setConfigScope(customerConfigScope)
              .addAnomalyDetectionConfigs(
                  AnomalyDetectionConfig.newBuilder()
                      .setConfigStatus(
                          AnomalyConfigStatusChange.newBuilder()
                              .setDisabled(false)
                              .setInternal(false)
                              .build())
                      .setVolumetricAnomalyDetectionConfig(
                          VolumetricAnomalyDetectionConfig.newBuilder()
                              .setApiCallSpike(ApiCallSpikeAnomalyConfig.newBuilder())
                              .build()))
              .build();

      updateDetectorConfig(detectionConfig2);
      updateScopedAnomalyConfigStatus(
          RequestContext.forTenantId(TENANT_ID),
          customerConfigScope,
          AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build(),
          Optional.empty());

      detectionConfig2 = fetchGlobalResolvedDetectorConfig(customerConfigScope);

      assertEquals(
          AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(false).build(),
          detectionConfig2.getAnomalyDetectionConfigsList().get(0).getConfigStatus());
    }
    {
      detectionConfig3 =
          ScopedAnomalyDetectionConfig.newBuilder()
              .setConfigScope(customerConfigScope)
              .addAnomalyDetectionConfigs(
                  AnomalyDetectionConfig.newBuilder()
                      .setConfigStatus(
                          AnomalyConfigStatusChange.newBuilder()
                              .setDisabled(true)
                              .setInternal(false)
                              .build())
                      .setVolumetricAnomalyDetectionConfig(
                          VolumetricAnomalyDetectionConfig.newBuilder()
                              .setApiCallSpike(ApiCallSpikeAnomalyConfig.newBuilder())
                              .build()))
              .build();

      updateDetectorConfig(detectionConfig3);
      updateScopedAnomalyConfigStatus(
          RequestContext.forTenantId(TENANT_ID),
          customerConfigScope,
          AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(false).build(),
          Optional.empty());

      detectionConfig3 = fetchGlobalResolvedDetectorConfig(customerConfigScope);

      assertEquals(
          AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(false).build(),
          detectionConfig3.getAnomalyDetectionConfigsList().get(0).getConfigStatus());
    }
  }

  @Test
  void testGetAllGlobalResolvedScopedAnomalyDetectionConfig() {
    ScopedAnomalyDetectionConfig detectionConfig1, detectionConfig2, detectionConfig3;
    assertThrows(
        RuntimeException.class,
        () -> fetchGlobalResolvedDetectorConfig(AnomalyConfigScope.newBuilder().build()));
    assertThrows(
        Exception.class,
        () ->
            updateDetectorConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(AnomalyConfigScope.newBuilder().build())
                    .build()));

    {
      detectionConfig1 =
          ScopedAnomalyDetectionConfig.newBuilder()
              .setConfigScope(customerConfigScope)
              .addAnomalyDetectionConfigs(
                  AnomalyDetectionConfig.newBuilder()
                      .setConfigStatus(
                          AnomalyConfigStatusChange.newBuilder()
                              .setDisabled(false)
                              .setInternal(true)
                              .build())
                      .setVolumetricAnomalyDetectionConfig(
                          VolumetricAnomalyDetectionConfig.newBuilder()
                              .setApiCallSpike(ApiCallSpikeAnomalyConfig.newBuilder())
                              .build()))
              .build();

      updateDetectorConfig(detectionConfig1);

      updateScopedAnomalyConfigStatus(
          RequestContext.forTenantId(TENANT_ID),
          customerConfigScope,
          AnomalyConfigStatusChange.newBuilder().setDisabled(false).setInternal(true).build(),
          Optional.empty());

      GetAnomalyDetectionConfigsFilter filter =
          GetAnomalyDetectionConfigsFilter.newBuilder()
              .addAnomalyDetectionConfigTypes(ANOMALY_DETECTION_CONFIG_TYPE_VOLUMETRIC)
              .build();

      List<ScopedAnomalyDetectionConfig> detectionConfigs1 =
          fetchAllGlobalResolvedDetectorConfig(filter);
      assertEquals(1, detectionConfigs1.size());
      detectionConfig1 = getScopedAnomalyDetectionConfig(detectionConfigs1, customerConfigScope);

      assertEquals(
          AnomalyConfigStatusChange.newBuilder().setDisabled(false).setInternal(true).build(),
          detectionConfig1.getAnomalyDetectionConfigsList().get(0).getConfigStatus());
    }
    {
      detectionConfig2 =
          ScopedAnomalyDetectionConfig.newBuilder()
              .setConfigScope(customerConfigScope)
              .addAnomalyDetectionConfigs(
                  AnomalyDetectionConfig.newBuilder()
                      .setConfigStatus(
                          AnomalyConfigStatusChange.newBuilder()
                              .setDisabled(false)
                              .setInternal(false)
                              .build())
                      .setVolumetricAnomalyDetectionConfig(
                          VolumetricAnomalyDetectionConfig.newBuilder()
                              .setApiCallSpike(ApiCallSpikeAnomalyConfig.newBuilder())
                              .build()))
              .build();

      updateDetectorConfig(detectionConfig2);
      updateScopedAnomalyConfigStatus(
          RequestContext.forTenantId(TENANT_ID),
          customerConfigScope,
          AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build(),
          Optional.empty());

      GetAnomalyDetectionConfigsFilter filter =
          GetAnomalyDetectionConfigsFilter.newBuilder()
              .addAnomalyDetectionConfigTypes(ANOMALY_DETECTION_CONFIG_TYPE_VOLUMETRIC)
              .build();

      List<ScopedAnomalyDetectionConfig> detectionConfigs2 =
          fetchAllGlobalResolvedDetectorConfig(filter);

      detectionConfig2 = getScopedAnomalyDetectionConfig(detectionConfigs2, customerConfigScope);

      assertEquals(
          AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(false).build(),
          detectionConfig2.getAnomalyDetectionConfigsList().get(0).getConfigStatus());
    }
    {
      detectionConfig3 =
          ScopedAnomalyDetectionConfig.newBuilder()
              .setConfigScope(customerConfigScope)
              .addAnomalyDetectionConfigs(
                  AnomalyDetectionConfig.newBuilder()
                      .setConfigStatus(
                          AnomalyConfigStatusChange.newBuilder()
                              .setDisabled(false)
                              .setInternal(true)
                              .build())
                      .setVolumetricAnomalyDetectionConfig(
                          VolumetricAnomalyDetectionConfig.newBuilder()
                              .setApiCallSpike(ApiCallSpikeAnomalyConfig.newBuilder())
                              .build()))
              .build();

      updateDetectorConfig(detectionConfig3);
      updateScopedAnomalyConfigStatus(
          RequestContext.forTenantId(TENANT_ID),
          customerConfigScope,
          AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build(),
          Optional.empty());

      GetAnomalyDetectionConfigsFilter filter =
          GetAnomalyDetectionConfigsFilter.newBuilder()
              .addAnomalyDetectionConfigTypes(ANOMALY_DETECTION_CONFIG_TYPE_VOLUMETRIC)
              .build();

      List<ScopedAnomalyDetectionConfig> detectionConfigs3 =
          fetchAllGlobalResolvedDetectorConfig(filter);
      assertEquals(1, detectionConfigs3.size());
      detectionConfig3 = getScopedAnomalyDetectionConfig(detectionConfigs3, customerConfigScope);

      assertEquals(
          AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build(),
          detectionConfig3.getAnomalyDetectionConfigsList().get(0).getConfigStatus());
    }
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

  private ScopedAnomalyDetectionConfig fetchGlobalResolvedDetectorConfig(
      AnomalyConfigScope configScope) {
    return RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                configServiceStub
                    .getGlobalResolvedScopedAnomalyDetectionConfig(
                        GetGlobalResolvedScopedAnomalyDetectionConfigRequest.newBuilder()
                            .setConfigScope(configScope)
                            .setFilter(
                                GetAnomalyDetectionConfigsFilter.newBuilder()
                                    .addAnomalyDetectionConfigTypes(
                                        ANOMALY_DETECTION_CONFIG_TYPE_VOLUMETRIC)
                                    .build())
                            .build())
                    .getScopedAnomalyDetectionConfig());
  }

  private List<ScopedAnomalyDetectionConfig> fetchAllGlobalResolvedDetectorConfig(
      GetAnomalyDetectionConfigsFilter filter) {
    return RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                configServiceStub
                    .getAllGlobalResolvedScopedAnomalyDetectionConfigs(
                        GetAllGlobalResolvedScopedAnomalyDetectionConfigsRequest.newBuilder()
                            .setFilter(filter)
                            .build())
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
                    globalConfigServiceStub.updateScopedAnomalyGlobalConfigStatus(
                        UpdateScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                            .setScopedConfig(scopedConfigBuilder)
                            .build()))
            .getScopedConfig();
    assertEquals(scope, updatedConfig.getConfigScope());
    return updatedConfig.getConfigStatus();
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
