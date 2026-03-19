package ai.traceable.anomaly.config.service.modsec.protection.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.global.ruleinfo.WebAppRuleInfoProvider;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.modsec.ModsecConfigServiceConfig;
import ai.traceable.anomaly.config.service.modsec.rules.ModsecManager;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.RuleEvaluationPoint;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAllDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.CorazaEngineVersion;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfig;
import ai.traceable.anomaly.config.service.v1.global.ModsecEvaluationEngineConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.modsec.GetWebAppEvaluationConfigContextRequest;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import ai.traceable.protection.engine.config.webapp.v1.SecRuleProcessorConfig;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationConfig;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationConfigContext;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationRulesContext;
import ai.traceable.protection.processing.common.v1.CustomerScope;
import ai.traceable.protection.processing.common.v1.Entity;
import ai.traceable.protection.processing.common.v1.EntityScope;
import ai.traceable.protection.processing.common.v1.EntityType;
import ai.traceable.protection.processing.common.v1.Scope;
import ai.traceable.protection.processing.common.v1.ScopeContext;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class WebAppEvaluationConfigContextManagerImplTest {

  private static final String TENANT_ID = "test-tenant";
  private static final String API_ID = "test-api";
  private static final String API_NAME = "test-api-name";
  private static final String SERVICE_ID = "test-service";
  private static final String SERVICE_NAME = "test-service-name";
  private static final String ENVIRONMENT_ID = "test-env";
  private static final ScopeContext API_SCOPE_CONTEXT =
      ScopeContext.newBuilder()
          .addScopes(
              Scope.newBuilder()
                  .setEntityScope(
                      EntityScope.newBuilder()
                          .setEntityType(EntityType.ENTITY_TYPE_API)
                          .addEntities(
                              Entity.newBuilder().setId(API_ID).setName(API_NAME).build())))
          .addScopes(
              Scope.newBuilder()
                  .setEntityScope(
                      EntityScope.newBuilder()
                          .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                          .addEntities(
                              Entity.newBuilder().setId(SERVICE_ID).setName(SERVICE_NAME).build())))
          .addScopes(
              Scope.newBuilder()
                  .setEntityScope(
                      EntityScope.newBuilder()
                          .setEntityType(EntityType.ENTITY_TYPE_ENVIRONMENT)
                          .addEntities(
                              Entity.newBuilder()
                                  .setId(ENVIRONMENT_ID)
                                  .setName(ENVIRONMENT_ID)
                                  .build())))
          .addScopes(Scope.newBuilder().setCustomerScope(CustomerScope.getDefaultInstance()))
          .build();
  private static final ScopeContext ENVIRONMENT_SCOPE_CONTEXT =
      ScopeContext.newBuilder()
          .addScopes(
              Scope.newBuilder()
                  .setEntityScope(
                      EntityScope.newBuilder()
                          .setEntityType(EntityType.ENTITY_TYPE_ENVIRONMENT)
                          .addEntities(
                              Entity.newBuilder()
                                  .setId(ENVIRONMENT_ID)
                                  .setName(ENVIRONMENT_ID)
                                  .build())))
          .addScopes(Scope.newBuilder().setCustomerScope(CustomerScope.getDefaultInstance()))
          .build();
  private static final ScopeContext CUSTOMER_SCOPE_CONTEXT =
      ScopeContext.newBuilder()
          .addScopes(Scope.newBuilder().setCustomerScope(CustomerScope.getDefaultInstance()))
          .build();

  private WebAppEvaluationConfigContextManagerImpl configContextManager;
  private RequestContext requestContext;
  private CachedApiMappingProvider apiMappingProvider;
  private CachedServiceMappingProvider serviceMappingProvider;
  private FeatureCachingClient featureCachingClient;
  private WebAppRuleInfoProvider webAppRuleInfoProvider;
  private AnomalyDetectionConfigManager anomalyDetectionConfigManager;

  @BeforeEach
  void setUp() {
    apiMappingProvider = mock(CachedApiMappingProvider.class);
    serviceMappingProvider = mock(CachedServiceMappingProvider.class);
    featureCachingClient = mock(FeatureCachingClient.class);
    webAppRuleInfoProvider = mock(WebAppRuleInfoProvider.class);
    ApiIdentifierEntity apiEntity =
        new ApiIdentifierEntity(API_ID, API_NAME, "/api-path", List.of("/api/path/.*"), List.of());
    when(apiMappingProvider.getApiIdentifierEntities(any(RequestContext.class), anySet()))
        .thenReturn(Map.of(API_ID, Optional.of(apiEntity)));
    ServiceIdentifierEntity serviceEntity =
        new ServiceIdentifierEntity(SERVICE_NAME, Optional.of(ENVIRONMENT_ID));
    when(serviceMappingProvider.getServiceIdentifierEntities(any(RequestContext.class), anySet()))
        .thenReturn(Map.of(SERVICE_ID, Optional.of(serviceEntity)));

    requestContext = RequestContext.forTenantId(TENANT_ID);
    ModsecManager modsecManager = mock(ModsecManager.class);
    ModsecRulesRegistry modsecRulesRegistry = mock(ModsecRulesRegistry.class);
    anomalyDetectionConfigManager = mock(AnomalyDetectionConfigManager.class);
    GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager =
        mock(GlobalAnomalyConfigStatusManager.class);

    when(modsecManager.getModsecCrsRules(
            any(),
            eq(ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3),
            anyBoolean(),
            any(),
            anyBoolean()))
        .thenReturn(
            ModsecManager.ModsecCrsRules.builder().aggregatedModsecBlob("defaultBlob").build());
    when(modsecManager.getModsecCrsRules(
            any(),
            eq(ModsecRuleVersion.MODSEC_RULE_VERSION_SENSITIVE_AGENT_CORAZA_V3),
            anyBoolean(),
            any(),
            anyBoolean()))
        .thenReturn(
            ModsecManager.ModsecCrsRules.builder().aggregatedModsecBlob("sensitiveBlob").build());
    when(modsecManager.getModsecCrsRules(
            Mockito.eq(
                List.of(
                    AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
                    AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)),
            eq(ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3),
            anyBoolean(),
            any(),
            anyBoolean()))
        .thenReturn(
            ModsecManager.ModsecCrsRules.builder()
                .aggregatedModsecBlob("onlyStandardRulesBlob")
                .build());

    when(modsecManager.getModsecCrsRules(
            Mockito.eq(List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR)),
            eq(ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3),
            anyBoolean(),
            any(),
            anyBoolean()))
        .thenReturn(
            ModsecManager.ModsecCrsRules.builder()
                .aggregatedModsecBlob("onlyAggressiveRulesBlob")
                .build());

    when(modsecRulesRegistry.getModsecRuleInfos(any(), anyBoolean()))
        .thenReturn(
            Map.of(
                "rule1",
                AnomalyRuleInfo.newBuilder()
                    .setRuleId("rule1")
                    .addSubRuleInfos(AnomalySubRuleInfo.newBuilder().setRuleId("subRule1"))
                    .addSubRuleInfos(AnomalySubRuleInfo.newBuilder().setRuleId("subRule2"))
                    .build(),
                "rule2",
                AnomalyRuleInfo.newBuilder()
                    .setRuleId("rule2")
                    .addSubRuleInfos(AnomalySubRuleInfo.newBuilder().setRuleId("subRule3"))
                    .build()));

    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(
            List.of(getApiScopedAnomalyDetectionConfig(), getEnvScopedAnomalyDetectionConfig()));

    when(globalAnomalyConfigStatusManager.getAllScopedAnomalyConfigStatusConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(getTenantScopedAnomalyConfigStatus()));
    when(featureCachingClient.isProtectionEngineWebAppProtectionEnabledForTenant(any()))
        .thenReturn(true);
    ModsecConfigServiceConfig modsecConfigServiceConfig = mock(ModsecConfigServiceConfig.class);
    when(modsecConfigServiceConfig.getWebAppConfigContextCacheMaxSize()).thenReturn(400);
    when(modsecConfigServiceConfig.getWebAppConfigContextCacheRefreshAfterWriteDuration())
        .thenReturn(Duration.ofMinutes(5));
    when(modsecConfigServiceConfig.getWebAppConfigContextCacheThreadPoolSize()).thenReturn(4);
    @SuppressWarnings("unchecked")
    KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener =
        mock(KafkaLiveEventListener.class);
    configContextManager =
        new WebAppEvaluationConfigContextManagerImpl(
            modsecManager,
            modsecRulesRegistry,
            anomalyDetectionConfigManager,
            globalAnomalyConfigStatusManager,
            featureCachingClient,
            webAppRuleInfoProvider,
            ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3,
            serviceMappingProvider,
            apiMappingProvider,
            modsecConfigServiceConfig,
            kafkaLiveEventListener);
  }

  private ScopedAnomalyConfigStatus getTenantScopedAnomalyConfigStatus() {
    return ScopedAnomalyConfigStatus.newBuilder()
        .setConfigScope(
            AnomalyConfigScope.newBuilder()
                .setCustomerScope(AnomalyCustomerScope.getDefaultInstance()))
        .setGlobalModsecConfig(
            GlobalModsecConfig.newBuilder()
                .setModsecEvaluationEngineConfig(
                    ModsecEvaluationEngineConfig.newBuilder()
                        .setCorazaEngineVersion(
                            CorazaEngineVersion.CORAZA_ENGINE_VERSION_LATEST_STABLE)))
        .build();
  }

  private ScopedAnomalyDetectionConfig getApiScopedAnomalyDetectionConfig() {
    return ScopedAnomalyDetectionConfig.newBuilder()
        .setConfigScope(
            AnomalyConfigScope.newBuilder()
                .setApiScope(
                    AnomalyApiScope.newBuilder()
                        .setId(API_ID)
                        .setServiceScope(
                            AnomalyServiceScope.newBuilder()
                                .setId(SERVICE_ID)
                                .setEnvironmentScope(
                                    AnomalyEnvironmentScope.newBuilder()
                                        .setEnvironmentId(ENVIRONMENT_ID))
                                .build())))
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setModsecurityAnomalyDetectionConfig(
                    ModsecurityAnomalyDetectionConfig.newBuilder()
                        .setModsecAnomalyRule(
                            ModsecurityAnomalyRuleConfig.newBuilder()
                                .setAnomalyRuleId("rule1")
                                .addSubRuleConfigs(
                                    AnomalySubRuleConfig.newBuilder()
                                        .setSubRuleId("subRule1")
                                        .setAnomalyRuleAction(
                                            AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK))
                                .addSubRuleConfigs(
                                    AnomalySubRuleConfig.newBuilder()
                                        .setSubRuleId("subRule2")
                                        .setAnomalyRuleAction(
                                            AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR)))))
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true))
                .setModsecurityAnomalyDetectionConfig(
                    ModsecurityAnomalyDetectionConfig.newBuilder()
                        .setModsecAnomalyRule(
                            ModsecurityAnomalyRuleConfig.newBuilder()
                                .setAnomalyRuleId("rule2")
                                .addSubRuleConfigs(
                                    AnomalySubRuleConfig.newBuilder()
                                        .setSubRuleId("subRule3")
                                        .setAnomalyRuleAction(
                                            AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK)))))
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setModsecurityAnomalyDetectionConfig(
                    ModsecurityAnomalyDetectionConfig.newBuilder()
                        .setModsecAllDetection(
                            ModsecurityAllDetectionConfig.newBuilder()
                                .setModsecRuleVersion(
                                    ModsecRuleVersion
                                        .MODSEC_RULE_VERSION_SENSITIVE_AGENT_CORAZA_V3))))
        .build();
  }

  private ScopedAnomalyDetectionConfig getEnvScopedAnomalyDetectionConfig() {
    return ScopedAnomalyDetectionConfig.newBuilder()
        .setConfigScope(
            AnomalyConfigScope.newBuilder()
                .setEnvironmentScope(
                    AnomalyEnvironmentScope.newBuilder().setEnvironmentId(ENVIRONMENT_ID)))
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setModsecurityAnomalyDetectionConfig(
                    ModsecurityAnomalyDetectionConfig.newBuilder()
                        .setModsecAnomalyRule(
                            ModsecurityAnomalyRuleConfig.newBuilder()
                                .setAnomalyRuleId("rule1")
                                .addSubRuleConfigs(
                                    AnomalySubRuleConfig.newBuilder()
                                        .setSubRuleId("subRule1")
                                        .setAnomalyRuleAction(
                                            AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR))
                                .addSubRuleConfigs(
                                    AnomalySubRuleConfig.newBuilder()
                                        .setSubRuleId("subRule2")
                                        .setAnomalyRuleAction(
                                            AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR)))))
        .build();
  }

  @Test
  void testEdgeWebAppEvaluationConfigContext() {
    WebAppEvaluationConfigContext result =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    assertEquals(2, result.getEvaluationConfigsList().size());
    WebAppEvaluationConfig evaluationConfig1 = result.getEvaluationConfigs(0);
    assertEquals(API_SCOPE_CONTEXT, evaluationConfig1.getScopeContext());
    assertEquals(List.of("subRule2", "subRule3"), evaluationConfig1.getDisabledSecRuleIdsList());

    WebAppEvaluationConfig evaluationConfig2 = result.getEvaluationConfigs(1);
    assertEquals(ENVIRONMENT_SCOPE_CONTEXT, evaluationConfig2.getScopeContext());
    assertEquals(
        List.of("subRule1", "subRule2", "subRule3"), evaluationConfig2.getDisabledSecRuleIdsList());

    assertEquals(3, result.getSecRuleProcessorConfigsList().size());
    SecRuleProcessorConfig secRuleProcessorConfig = result.getSecRuleProcessorConfigs(0);
    assertEquals(API_SCOPE_CONTEXT, secRuleProcessorConfig.getScopeContext());
    assertEquals(
        ai.traceable.protection.processor.secrules.v1.CorazaEngineVersion
            .CORAZA_ENGINE_VERSION_LATEST_STABLE,
        secRuleProcessorConfig
            .getProcessorDetails()
            .getCorazaRuleProcessor()
            .getCorazaEngineVersion());

    assertEquals(2, result.getWebAppEvaluationRulesContextsList().size());
    WebAppEvaluationRulesContext rulesContext1 = result.getWebAppEvaluationRulesContexts(0);
    assertEquals(API_SCOPE_CONTEXT, rulesContext1.getScopeContext());
    assertEquals(HashUtil.calculateSHA256("sensitiveBlob"), rulesContext1.getCrsRulesBlobId());

    WebAppEvaluationRulesContext rulesContext2 = result.getWebAppEvaluationRulesContexts(1);
    assertEquals(ENVIRONMENT_SCOPE_CONTEXT, rulesContext2.getScopeContext());
    assertEquals(
        HashUtil.calculateSHA256("onlyStandardRulesBlob"), rulesContext2.getCrsRulesBlobId());
  }

  @Test
  void testEdgeWebAppEvaluationConfigContext_WithConfigScopeFiltersScopes() {
    WebAppEvaluationConfigContext result =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setEnvironmentScope(
                            AnomalyEnvironmentScope.newBuilder().setEnvironmentId(ENVIRONMENT_ID)))
                .build());

    assertEquals(1, result.getEvaluationConfigsList().size());
    WebAppEvaluationConfig evaluationConfig1 = result.getEvaluationConfigs(0);
    assertEquals(ENVIRONMENT_SCOPE_CONTEXT, evaluationConfig1.getScopeContext());
    assertEquals(
        List.of("subRule1", "subRule2", "subRule3"), evaluationConfig1.getDisabledSecRuleIdsList());

    assertEquals(2, result.getSecRuleProcessorConfigsList().size());

    assertEquals(1, result.getWebAppEvaluationRulesContextsList().size());
    WebAppEvaluationRulesContext rulesContext1 = result.getWebAppEvaluationRulesContexts(0);
    assertEquals(ENVIRONMENT_SCOPE_CONTEXT, rulesContext1.getScopeContext());
    assertEquals(
        HashUtil.calculateSHA256("onlyStandardRulesBlob"), rulesContext1.getCrsRulesBlobId());
  }

  @Test
  void testBackwardCompat_NoConfigScopeReturnsAllScopes() {
    WebAppEvaluationConfigContext resultWithoutScope =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());
    assertEquals(2, resultWithoutScope.getEvaluationConfigsList().size());
    assertEquals(API_SCOPE_CONTEXT, resultWithoutScope.getEvaluationConfigs(0).getScopeContext());
    assertEquals(
        ENVIRONMENT_SCOPE_CONTEXT, resultWithoutScope.getEvaluationConfigs(1).getScopeContext());
    assertEquals(3, resultWithoutScope.getSecRuleProcessorConfigsList().size());
    assertEquals(2, resultWithoutScope.getWebAppEvaluationRulesContextsList().size());
  }

  @Test
  void testScopedRequest_ReturnsFewerResultsThanUnscoped() {
    WebAppEvaluationConfigContext unscopedResult =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .build());

    WebAppEvaluationConfigContext scopedResult =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setEnvironmentScope(
                            AnomalyEnvironmentScope.newBuilder().setEnvironmentId(ENVIRONMENT_ID)))
                .build());

    assertEquals(1, unscopedResult.getEvaluationConfigsList().size());
    assertEquals(0, scopedResult.getEvaluationConfigsList().size());
    assertEquals(3, unscopedResult.getSecRuleProcessorConfigsList().size());
    assertEquals(2, scopedResult.getSecRuleProcessorConfigsList().size());
  }

  @Test
  void testCachingBehavior_ScopedAndUnscopedAreDifferentCacheEntries() {
    WebAppEvaluationConfigContext unscopedResult =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    WebAppEvaluationConfigContext scopedResult =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setEnvironmentScope(
                            AnomalyEnvironmentScope.newBuilder().setEnvironmentId(ENVIRONMENT_ID)))
                .build());
    assertNotSame(unscopedResult, scopedResult);
    assertEquals(2, unscopedResult.getEvaluationConfigsList().size());
    assertEquals(1, scopedResult.getEvaluationConfigsList().size());
    verify(anomalyDetectionConfigManager, times(2))
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(any(RequestContext.class), any());
  }

  @Test
  void testExplicitCustomerScope_ReturnsOnlyCustomerScopedConfigs() {
    WebAppEvaluationConfigContext result =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setCustomerScope(AnomalyCustomerScope.getDefaultInstance()))
                .build());
    assertEquals(0, result.getEvaluationConfigsList().size());
    assertEquals(1, result.getSecRuleProcessorConfigsList().size());
    assertEquals(CUSTOMER_SCOPE_CONTEXT, result.getSecRuleProcessorConfigs(0).getScopeContext());
  }

  @Test
  void testPlatformWebAppEvaluationConfigContext() {
    WebAppEvaluationConfigContext result =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .build());

    assertEquals(1, result.getEvaluationConfigsList().size());
    WebAppEvaluationConfig evaluationConfig = result.getEvaluationConfigs(0);
    assertEquals(API_SCOPE_CONTEXT, evaluationConfig.getScopeContext());
    assertEquals(List.of("subRule3"), evaluationConfig.getDisabledSecRuleIdsList());

    assertEquals(3, result.getSecRuleProcessorConfigsList().size());
    SecRuleProcessorConfig secRuleProcessorConfig = result.getSecRuleProcessorConfigs(0);
    assertEquals(API_SCOPE_CONTEXT, secRuleProcessorConfig.getScopeContext());
    assertEquals(
        ai.traceable.protection.processor.secrules.v1.CorazaEngineVersion
            .CORAZA_ENGINE_VERSION_LATEST_STABLE,
        secRuleProcessorConfig
            .getProcessorDetails()
            .getCorazaRuleProcessor()
            .getCorazaEngineVersion());

    assertEquals(2, result.getWebAppEvaluationRulesContextsList().size());
    WebAppEvaluationRulesContext rulesContext1 = result.getWebAppEvaluationRulesContexts(0);
    assertEquals(API_SCOPE_CONTEXT, rulesContext1.getScopeContext());
    assertEquals(HashUtil.calculateSHA256("sensitiveBlob"), rulesContext1.getCrsRulesBlobId());

    WebAppEvaluationRulesContext rulesContext2 = result.getWebAppEvaluationRulesContexts(1);
    assertEquals(ENVIRONMENT_SCOPE_CONTEXT, rulesContext2.getScopeContext());
    assertEquals(HashUtil.calculateSHA256("defaultBlob"), rulesContext2.getCrsRulesBlobId());
  }

  @Test
  void testWebAppEvaluationRulesContextForStandardRulesFilter() {
    WebAppEvaluationConfigContext result =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .addAllSubRuleTypes(
                    List.of(
                        AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
                        AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK))
                .build());

    assertEquals(2, result.getWebAppEvaluationRulesContextsList().size());
    WebAppEvaluationRulesContext rulesContext1 = result.getWebAppEvaluationRulesContexts(0);
    assertEquals(API_SCOPE_CONTEXT, rulesContext1.getScopeContext());
    assertEquals(HashUtil.calculateSHA256("sensitiveBlob"), rulesContext1.getCrsRulesBlobId());

    WebAppEvaluationRulesContext rulesContext2 = result.getWebAppEvaluationRulesContexts(1);
    assertEquals(ENVIRONMENT_SCOPE_CONTEXT, rulesContext2.getScopeContext());
    assertEquals(
        HashUtil.calculateSHA256("onlyStandardRulesBlob"), rulesContext2.getCrsRulesBlobId());
  }

  @Test
  void testWebAppConfigContextForAggressiveRulesFilter() {
    WebAppEvaluationConfigContext result =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .addAllSubRuleTypes(List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR))
                .build());

    assertEquals(2, result.getWebAppEvaluationRulesContextsList().size());
    WebAppEvaluationRulesContext rulesContext1 = result.getWebAppEvaluationRulesContexts(0);
    assertEquals(API_SCOPE_CONTEXT, rulesContext1.getScopeContext());
    assertEquals(HashUtil.calculateSHA256("sensitiveBlob"), rulesContext1.getCrsRulesBlobId());

    WebAppEvaluationRulesContext rulesContext2 = result.getWebAppEvaluationRulesContexts(1);
    assertEquals(ENVIRONMENT_SCOPE_CONTEXT, rulesContext2.getScopeContext());
    assertEquals(
        HashUtil.calculateSHA256("onlyAggressiveRulesBlob"), rulesContext2.getCrsRulesBlobId());
  }

  @Test
  void testWebAppConfigContextForOrdering() {
    // testing ordering of lists in WebAppEvaluationConfigContext
    // we expect following sorted order for all lists:
    // api > service > environment > customer

    // setup
    String testEnv2 = "test-env-2";
    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setEnvironmentScope(
                        AnomalyEnvironmentScope.newBuilder().setEnvironmentId(testEnv2)))
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAnomalyRule(
                                ModsecurityAnomalyRuleConfig.newBuilder()
                                    .setAnomalyRuleId("rule1")
                                    .addSubRuleConfigs(
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("subRule1")
                                            .setAnomalyRuleAction(
                                                AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK))
                                    .addSubRuleConfigs(
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("subRule2")
                                            .setAnomalyRuleAction(
                                                AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR)))))
            .build();
    ScopeContext SECOND_ENVIRONMENT_SCOPE_CONTEXT =
        ScopeContext.newBuilder()
            .addScopes(
                Scope.newBuilder()
                    .setEntityScope(
                        EntityScope.newBuilder()
                            .setEntityType(EntityType.ENTITY_TYPE_ENVIRONMENT)
                            .addEntities(
                                Entity.newBuilder().setId(testEnv2).setName(testEnv2).build())))
            .addScopes(Scope.newBuilder().setCustomerScope(CustomerScope.getDefaultInstance()))
            .build();
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(
            List.of(
                getApiScopedAnomalyDetectionConfig(),
                getEnvScopedAnomalyDetectionConfig(),
                scopedAnomalyDetectionConfig));
    // action
    WebAppEvaluationConfigContext result =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());
    // verify
    assertEquals(3, result.getEvaluationConfigsList().size());

    WebAppEvaluationConfig evaluationConfig1 = result.getEvaluationConfigs(0);
    assertEquals(API_SCOPE_CONTEXT, evaluationConfig1.getScopeContext());
    assertEquals(List.of("subRule2", "subRule3"), evaluationConfig1.getDisabledSecRuleIdsList());

    WebAppEvaluationConfig evaluationConfig2 = result.getEvaluationConfigs(1);
    assertEquals(ENVIRONMENT_SCOPE_CONTEXT, evaluationConfig2.getScopeContext());
    assertEquals(
        List.of("subRule1", "subRule2", "subRule3"), evaluationConfig2.getDisabledSecRuleIdsList());

    WebAppEvaluationConfig evaluationConfig3 = result.getEvaluationConfigs(2);
    assertEquals(SECOND_ENVIRONMENT_SCOPE_CONTEXT, evaluationConfig3.getScopeContext());
    assertEquals(List.of("subRule2", "subRule3"), evaluationConfig3.getDisabledSecRuleIdsList());
  }

  @Test
  void testCachingBehavior_CacheHit() {
    // First call - should compute and cache
    WebAppEvaluationConfigContext result1 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    // Second call with same parameters - should return cached result
    WebAppEvaluationConfigContext result2 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    // Results should be the same instance (from cache)
    assertSame(result1, result2);
    assertEquals(result1, result2);

    // Verify the underlying managers were only called once (during first call)
    verify(anomalyDetectionConfigManager, times(1))
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(any(RequestContext.class), any());
  }

  @Test
  void testCachingBehavior_DifferentRequestParameters() {
    // Call with EDGE evaluation point
    WebAppEvaluationConfigContext result1 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    // Call with PLATFORM evaluation point - should be a cache miss
    WebAppEvaluationConfigContext result2 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .build());

    // Results should be different instances (different cache keys)
    assertNotSame(result1, result2);

    // Verify the underlying managers were called twice (once for each cache key)
    verify(anomalyDetectionConfigManager, times(2))
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(any(RequestContext.class), any());
  }

  @Test
  void testCachingBehavior_DifferentConfigScopeForEdgeIsCacheMiss() {
    WebAppEvaluationConfigContext result1 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setEnvironmentScope(
                            AnomalyEnvironmentScope.newBuilder().setEnvironmentId(ENVIRONMENT_ID)))
                .build());

    WebAppEvaluationConfigContext result2 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setEnvironmentScope(
                            AnomalyEnvironmentScope.newBuilder().setEnvironmentId("different-env")))
                .build());

    assertNotSame(result1, result2);
    verify(anomalyDetectionConfigManager, times(2))
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(any(RequestContext.class), any());
  }

  @Test
  void testCachingBehavior_DifferentSubRuleTypes() {
    // Call with specific sub rule types
    WebAppEvaluationConfigContext result1 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .addAllSubRuleTypes(
                    List.of(
                        AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
                        AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK))
                .build());

    // Call with different sub rule types - should be a cache miss
    WebAppEvaluationConfigContext result2 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .addAllSubRuleTypes(List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR))
                .build());

    // Results should be different instances
    assertNotSame(result1, result2);

    // Verify the underlying managers were called twice
    verify(anomalyDetectionConfigManager, times(2))
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(any(RequestContext.class), any());
  }

  @Test
  void testCachingBehavior_SubRuleTypesOrderDoesNotMatter() {
    // Call with sub rule types in one order
    WebAppEvaluationConfigContext result1 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .addAllSubRuleTypes(
                    List.of(
                        AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK,
                        AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE))
                .build());

    // Call with same sub rule types in different order - should be a cache hit
    WebAppEvaluationConfigContext result2 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .addAllSubRuleTypes(
                    List.of(
                        AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
                        AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK))
                .build());

    // Results shouldn't be the same as the rule evaluation point is platform
    assertNotSame(result1, result2);

    // Verify the underlying managers were only called once
    verify(anomalyDetectionConfigManager, times(2))
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(any(RequestContext.class), any());
  }

  @Test
  void testInvalidateCacheForTenant() {
    // First call - should compute and cache
    WebAppEvaluationConfigContext result1 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    assertNotNull(result1);

    // Invalidate cache for this tenant
    configContextManager.invalidateCacheForTenant(TENANT_ID);

    // Second call - should recompute (cache was invalidated)
    WebAppEvaluationConfigContext result2 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    // Results should be different instances (cache was invalidated)
    assertNotSame(result1, result2);

    // Verify the underlying managers were called twice (once before invalidation, once after)
    verify(anomalyDetectionConfigManager, times(2))
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(any(RequestContext.class), any());
  }

  @Test
  void testInvalidateCacheForTenant_DoesNotAffectOtherTenants() {
    String otherTenantId = "other-tenant";
    RequestContext otherRequestContext = RequestContext.forTenantId(otherTenantId);

    // Call for first tenant
    WebAppEvaluationConfigContext result1 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    // Call for second tenant
    WebAppEvaluationConfigContext result2 =
        configContextManager.getWebAppEvaluationConfigContext(
            otherRequestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    // Invalidate cache for first tenant only
    configContextManager.invalidateCacheForTenant(TENANT_ID);

    // Call again for first tenant - should recompute
    WebAppEvaluationConfigContext result3 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    // Call again for second tenant - should return cached result
    WebAppEvaluationConfigContext result4 =
        configContextManager.getWebAppEvaluationConfigContext(
            otherRequestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    // First tenant: results should be different instances
    assertNotSame(result1, result3);

    // Second tenant: results should be the same instance (cache not invalidated)
    assertSame(result2, result4);
  }

  @Test
  void testInvalidateAllCache() {
    // Make multiple calls with different parameters to populate cache
    WebAppEvaluationConfigContext result1 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    WebAppEvaluationConfigContext result2 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .build());

    assertNotNull(result1);
    assertNotNull(result2);

    // Invalidate all cache entries
    configContextManager.invalidateAllCache();

    // Call again with same parameters - should recompute both
    WebAppEvaluationConfigContext result3 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    WebAppEvaluationConfigContext result4 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .build());

    // All results should be different instances (cache was cleared)
    assertNotSame(result1, result3);
    assertNotSame(result2, result4);

    // Verify the underlying managers were called 4 times total
    // (2 before invalidation, 2 after)
    verify(anomalyDetectionConfigManager, times(4))
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(any(RequestContext.class), any());
  }

  @Test
  void testCachingBehavior_DifferentTenants() {
    String otherTenantId = "other-tenant";
    RequestContext otherRequestContext = RequestContext.forTenantId(otherTenantId);

    // Call for first tenant
    WebAppEvaluationConfigContext result1 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    // Call for second tenant with same request parameters
    WebAppEvaluationConfigContext result2 =
        configContextManager.getWebAppEvaluationConfigContext(
            otherRequestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    // Results should be different instances (different tenants = different cache keys)
    assertNotSame(result1, result2);

    // Call again for first tenant - should return cached result
    WebAppEvaluationConfigContext result3 =
        configContextManager.getWebAppEvaluationConfigContext(
            requestContext,
            GetWebAppEvaluationConfigContextRequest.newBuilder()
                .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build());

    // Should be same instance as first call (cache hit)
    assertSame(result1, result3);

    // Verify the underlying managers were called twice (once per tenant)
    verify(anomalyDetectionConfigManager, times(2))
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(any(RequestContext.class), any());
  }
}
