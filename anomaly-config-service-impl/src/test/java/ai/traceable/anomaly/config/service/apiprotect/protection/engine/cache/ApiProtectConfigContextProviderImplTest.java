package ai.traceable.anomaly.config.service.apiprotect.protection.engine.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.apiprotect.ApiProtectConfigServiceConfig;
import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.RuleEvaluationPoint;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.RuleVersionData;
import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextRequest;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiProtectAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiProtectAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalApiConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionConfigContext;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectionRulesProvider;
import com.google.protobuf.Value;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiProtectConfigContextProviderImplTest {

  private static final String TENANT_ID = "test-tenant";
  private static final String API_ID = "test-api";
  private static final String SERVICE_ID = "test-service";
  private static final String ENVIRONMENT_ID = "test-env";

  private ApiProtectConfigContextProvider provider;
  private RequestContext requestContext;
  private AnomalyDetectionConfigManager anomalyDetectionConfigManager;
  private GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager;

  @BeforeEach
  void setUp() {
    anomalyDetectionConfigManager = mock(AnomalyDetectionConfigManager.class);
    globalAnomalyConfigStatusManager = mock(GlobalAnomalyConfigStatusManager.class);
    ApiProtectionRulesProvider apiProtectionRulesProvider = mock(ApiProtectionRulesProvider.class);
    FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
    CachedServiceMappingProvider cachedServiceMappingProvider =
        mock(CachedServiceMappingProvider.class);
    CachedApiMappingProvider cachedApiMappingProvider = mock(CachedApiMappingProvider.class);
    ApiProtectConfigServiceConfig config = mock(ApiProtectConfigServiceConfig.class);
    @SuppressWarnings("unchecked")
    KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener =
        mock(KafkaLiveEventListener.class);

    requestContext = RequestContext.forTenantId(TENANT_ID);
    when(config.getApiProtectConfigContextCacheMaxSize()).thenReturn(400);
    when(config.getApiProtectConfigContextCacheRefreshAfterWriteDuration())
        .thenReturn(Duration.ofMinutes(5));
    when(config.getApiProtectConfigContextCacheThreadPoolSize()).thenReturn(4);
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(Collections.emptyList());
    when(globalAnomalyConfigStatusManager.getAllScopedAnomalyConfigStatusConfigs(
            any(RequestContext.class), any()))
        .thenReturn(Collections.emptyList());
    when(featureCachingClient.isWAAPVersioningEnabledForTenant(any())).thenReturn(false);
    ServiceIdentifierEntity serviceEntity = mock(ServiceIdentifierEntity.class);
    when(serviceEntity.getServiceName()).thenReturn(SERVICE_ID);
    when(cachedServiceMappingProvider.getServiceIdentifierEntities(
            any(RequestContext.class), any()))
        .thenReturn(Map.of(SERVICE_ID, Optional.of(serviceEntity)));

    ApiIdentifierEntity apiEntity = mock(ApiIdentifierEntity.class);
    when(apiEntity.getApiName()).thenReturn(API_ID);
    when(cachedApiMappingProvider.getApiIdentifierEntities(any(RequestContext.class), any()))
        .thenReturn(Map.of(API_ID, Optional.of(apiEntity)));

    // Configure the rules provider to return empty list (rules will be built from detection
    // configs)
    when(apiProtectionRulesProvider.getApiProtectVersionedRules(any()))
        .thenReturn(Collections.emptyList());

    provider =
        new ApiProtectConfigContextCacheProvider(
            config,
            kafkaLiveEventListener,
            featureCachingClient,
            anomalyDetectionConfigManager,
            globalAnomalyConfigStatusManager,
            apiProtectionRulesProvider,
            cachedServiceMappingProvider,
            cachedApiMappingProvider);
  }

  @Test
  void testGetApiProtectionConfigContext_NoConfigs_ReturnsEmptyContext() {
    GetApiProtectEvaluationConfigContextRequest request =
        GetApiProtectEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();
    ApiProtectionConfigContext result =
        provider.getApiProtectionConfigContext(requestContext, request);
    assertNotNull(result);
    assertEquals(0, result.getRuleContextsList().size());
  }

  @Test
  void testGetApiProtectionConfigContext_EdgeRulesOnly() {
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(getApiScopedConfigWithBlockingRule()));

    when(globalAnomalyConfigStatusManager.getAllScopedAnomalyConfigStatusConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(getTenantScopedAnomalyConfigStatus()));

    GetApiProtectEvaluationConfigContextRequest request =
        GetApiProtectEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();
    ApiProtectionConfigContext result =
        provider.getApiProtectionConfigContext(requestContext, request);
    assertNotNull(result);
    // Debug: print what we got
    System.out.println("Result rule contexts count: " + result.getRuleContextsCount());
    if (result.getRuleContextsCount() > 0) {
      System.out.println(
          "First context rule count: " + result.getRuleContexts(0).getRuleConfigsCount());
    }
    assertEquals(1, result.getRuleContextsList().size(), "Expected 1 rule context");
    assertEquals(
        1, result.getRuleContexts(0).getRuleConfigsList().size(), "Expected 1 rule config");
    assertEquals("edge-rule-1", result.getRuleContexts(0).getRuleConfigs(0).getRuleId());
  }

  @Test
  void testGetApiProtectionConfigContext_PlatformRulesOnly() {
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(getApiScopedConfigWithDetectRule()));

    when(globalAnomalyConfigStatusManager.getAllScopedAnomalyConfigStatusConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(getTenantScopedAnomalyConfigStatus()));

    GetApiProtectEvaluationConfigContextRequest request =
        GetApiProtectEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();
    ApiProtectionConfigContext result =
        provider.getApiProtectionConfigContext(requestContext, request);
    assertNotNull(result);
    assertEquals(1, result.getRuleContextsList().size());
    assertEquals(1, result.getRuleContexts(0).getRuleConfigsList().size());
    assertEquals("platform-rule-1", result.getRuleContexts(0).getRuleConfigs(0).getRuleId());
  }

  @Test
  void testGetApiProtectionConfigContext_DisabledConfig_ReturnsEmpty() {
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(getDisabledConfig()));

    when(globalAnomalyConfigStatusManager.getAllScopedAnomalyConfigStatusConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(getTenantScopedAnomalyConfigStatus()));

    GetApiProtectEvaluationConfigContextRequest request =
        GetApiProtectEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();
    ApiProtectionConfigContext result =
        provider.getApiProtectionConfigContext(requestContext, request);
    assertNotNull(result);
    assertEquals(0, result.getRuleContextsList().size());
  }

  @Test
  void testGetApiProtectionConfigContext_ScopeFallback_UsesParentScopeConfig() {
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(getApiScopedConfigWithBlockingRule()));

    when(globalAnomalyConfigStatusManager.getAllScopedAnomalyConfigStatusConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(getTenantScopedAnomalyConfigStatus()));

    GetApiProtectEvaluationConfigContextRequest request =
        GetApiProtectEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    ApiProtectionConfigContext result =
        provider.getApiProtectionConfigContext(requestContext, request);

    assertNotNull(result);
    assertEquals(1, result.getRuleContextsList().size());
    // Should successfully resolve using the customer scope status via fallback
    assertEquals(1, result.getRuleContexts(0).getRuleConfigsList().size());
  }

  @Test
  void testCaching_SameRequest_ReturnsCachedResult() {
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(getApiScopedConfigWithBlockingRule()));

    when(globalAnomalyConfigStatusManager.getAllScopedAnomalyConfigStatusConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(getTenantScopedAnomalyConfigStatus()));

    GetApiProtectEvaluationConfigContextRequest request =
        GetApiProtectEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    ApiProtectionConfigContext result1 =
        provider.getApiProtectionConfigContext(requestContext, request);
    ApiProtectionConfigContext result2 =
        provider.getApiProtectionConfigContext(requestContext, request);
    assertNotNull(result1);
    assertNotNull(result2);
    verify(anomalyDetectionConfigManager, times(1))
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(any(RequestContext.class), any());
  }

  @Test
  void testCaching_DifferentRequests_ComputesSeparately() {
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(getApiScopedConfigWithBlockingRule()));

    when(globalAnomalyConfigStatusManager.getAllScopedAnomalyConfigStatusConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(getTenantScopedAnomalyConfigStatus()));

    GetApiProtectEvaluationConfigContextRequest edgeRequest =
        GetApiProtectEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    GetApiProtectEvaluationConfigContextRequest platformRequest =
        GetApiProtectEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();
    ApiProtectionConfigContext result1 =
        provider.getApiProtectionConfigContext(requestContext, edgeRequest);
    ApiProtectionConfigContext result2 =
        provider.getApiProtectionConfigContext(requestContext, platformRequest);
    assertNotNull(result1);
    assertNotNull(result2);
    verify(anomalyDetectionConfigManager, times(2))
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(any(RequestContext.class), any());
  }

  private ScopedAnomalyConfigStatus getTenantScopedAnomalyConfigStatus() {
    return ScopedAnomalyConfigStatus.newBuilder()
        .setConfigScope(
            AnomalyConfigScope.newBuilder()
                .setCustomerScope(AnomalyCustomerScope.getDefaultInstance()))
        .setGlobalApiConfig(
            GlobalApiConfig.newBuilder()
                .setRuleVersionData(
                    RuleVersionData.newBuilder()
                        .setCurrentVersion(RuleVersion.newBuilder().setVersion("v1.0"))))
        .build();
  }

  private ScopedAnomalyDetectionConfig getApiScopedConfigWithBlockingRule() {
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
                                        .setEnvironmentId(ENVIRONMENT_ID)))))
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setApiProtectAnomalyDetectionConfig(
                    ApiProtectAnomalyDetectionConfig.newBuilder()
                        .setApiProtectAnomalyRule(
                            ApiProtectAnomalyRuleConfig.newBuilder()
                                .setAnomalyRuleId("rule-1")
                                .addSubRuleConfigs(
                                    AnomalySubRuleConfig.newBuilder()
                                        .setSubRuleId("edge-rule-1")
                                        .setAnomalyRuleAction(
                                            AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK)
                                        .putConfigParams(
                                            "threshold",
                                            Value.newBuilder().setNumberValue(100).build())))))
        .build();
  }

  private ScopedAnomalyDetectionConfig getApiScopedConfigWithDetectRule() {
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
                                        .setEnvironmentId(ENVIRONMENT_ID)))))
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setApiProtectAnomalyDetectionConfig(
                    ApiProtectAnomalyDetectionConfig.newBuilder()
                        .setApiProtectAnomalyRule(
                            ApiProtectAnomalyRuleConfig.newBuilder()
                                .setAnomalyRuleId("rule-1")
                                .addSubRuleConfigs(
                                    AnomalySubRuleConfig.newBuilder()
                                        .setSubRuleId("platform-rule-1")
                                        .setAnomalyRuleAction(
                                            AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR)))))
        .build();
  }

  private ScopedAnomalyDetectionConfig getServiceScopedConfigWithBlockingRule() {
    return ScopedAnomalyDetectionConfig.newBuilder()
        .setConfigScope(
            AnomalyConfigScope.newBuilder()
                .setServiceScope(
                    AnomalyServiceScope.newBuilder()
                        .setId(SERVICE_ID)
                        .setEnvironmentScope(
                            AnomalyEnvironmentScope.newBuilder().setEnvironmentId(ENVIRONMENT_ID))))
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setApiProtectAnomalyDetectionConfig(
                    ApiProtectAnomalyDetectionConfig.newBuilder()
                        .setApiProtectAnomalyRule(
                            ApiProtectAnomalyRuleConfig.newBuilder()
                                .setAnomalyRuleId("service-rule")
                                .addSubRuleConfigs(
                                    AnomalySubRuleConfig.newBuilder()
                                        .setSubRuleId("service-edge-rule")
                                        .setAnomalyRuleAction(
                                            AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK)))))
        .build();
  }

  private ScopedAnomalyDetectionConfig getEnvironmentScopedConfigWithBlockingRule() {
    return ScopedAnomalyDetectionConfig.newBuilder()
        .setConfigScope(
            AnomalyConfigScope.newBuilder()
                .setEnvironmentScope(
                    AnomalyEnvironmentScope.newBuilder().setEnvironmentId(ENVIRONMENT_ID)))
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setApiProtectAnomalyDetectionConfig(
                    ApiProtectAnomalyDetectionConfig.newBuilder()
                        .setApiProtectAnomalyRule(
                            ApiProtectAnomalyRuleConfig.newBuilder()
                                .setAnomalyRuleId("env-rule")
                                .addSubRuleConfigs(
                                    AnomalySubRuleConfig.newBuilder()
                                        .setSubRuleId("env-edge-rule")
                                        .setAnomalyRuleAction(
                                            AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK)))))
        .build();
  }

  private ScopedAnomalyDetectionConfig getDisabledConfig() {
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
                                        .setEnvironmentId(ENVIRONMENT_ID)))))
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
                .setApiProtectAnomalyDetectionConfig(
                    ApiProtectAnomalyDetectionConfig.newBuilder()
                        .setApiProtectAnomalyRule(
                            ApiProtectAnomalyRuleConfig.newBuilder()
                                .setAnomalyRuleId("disabled-rule")
                                .addSubRuleConfigs(
                                    AnomalySubRuleConfig.newBuilder()
                                        .setSubRuleId("disabled-sub-rule")
                                        .setAnomalyRuleAction(
                                            AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK)))))
        .build();
  }
}
