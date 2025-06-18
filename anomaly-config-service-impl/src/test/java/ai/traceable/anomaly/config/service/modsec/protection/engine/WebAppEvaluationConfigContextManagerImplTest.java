package ai.traceable.anomaly.config.service.modsec.protection.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.modsec.rules.ModsecManager;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
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
import ai.traceable.anomaly.config.service.v1.modsec.RuleEvaluationPoint;
import ai.traceable.protection.engine.config.webapp.v1.SecRuleProcessorConfig;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationConfig;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationConfigContext;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationRulesContext;
import ai.traceable.protection.processing.common.v1.CustomerScope;
import ai.traceable.protection.processing.common.v1.EntityScope;
import ai.traceable.protection.processing.common.v1.EntityType;
import ai.traceable.protection.processing.common.v1.Scope;
import ai.traceable.protection.processing.common.v1.ScopeContext;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WebAppEvaluationConfigContextManagerImplTest {

  private static final String TENANT_ID = "test-tenant";
  private static final String API_ID = "test-api";
  private static final String ENVIRONMENT_ID = "test-env";
  private static final ScopeContext API_SCOPE_CONTEXT =
      ScopeContext.newBuilder()
          .addScopes(
              Scope.newBuilder()
                  .setEntityScope(
                      EntityScope.newBuilder()
                          .setEntityType(EntityType.ENTITY_TYPE_API)
                          .addEntityIds(API_ID)))
          .build();
  private static final ScopeContext ENVIRONMENT_SCOPE_CONTEXT =
      ScopeContext.newBuilder()
          .addScopes(
              Scope.newBuilder()
                  .setEntityScope(
                      EntityScope.newBuilder()
                          .setEntityType(EntityType.ENTITY_TYPE_ENVIRONMENT)
                          .addEntityIds(ENVIRONMENT_ID)))
          .build();
  private static final ScopeContext CUSTOMER_SCOPE_CONTEXT =
      ScopeContext.newBuilder()
          .addScopes(Scope.newBuilder().setCustomerScope(CustomerScope.getDefaultInstance()))
          .build();

  private WebAppEvaluationConfigContextManagerImpl configContextManager;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    requestContext = RequestContext.forTenantId(TENANT_ID);
    ModsecManager modsecManager = mock(ModsecManager.class);
    ModsecRulesRegistry modsecRulesRegistry = mock(ModsecRulesRegistry.class);
    AnomalyDetectionConfigManager anomalyDetectionConfigManager =
        mock(AnomalyDetectionConfigManager.class);
    GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager =
        mock(GlobalAnomalyConfigStatusManager.class);

    when(modsecManager.getModsecCrsRules(
            any(), eq(ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3), anyBoolean()))
        .thenReturn(
            ModsecManager.ModsecCrsRules.builder().aggregatedModsecBlob("defaultBlob").build());
    when(modsecManager.getModsecCrsRules(
            any(),
            eq(ModsecRuleVersion.MODSEC_RULE_VERSION_SENSITIVE_AGENT_CORAZA_V3),
            anyBoolean()))
        .thenReturn(
            ModsecManager.ModsecCrsRules.builder().aggregatedModsecBlob("sensitiveBlob").build());

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
            eq(requestContext), any()))
        .thenReturn(
            List.of(getApiScopedAnomalyDetectionConfig(), getEnvScopedAnomalyDetectionConfig()));

    when(globalAnomalyConfigStatusManager.getAllScopedAnomalyConfigStatusConfigs(
            eq(requestContext), any()))
        .thenReturn(List.of(getTenantScopedAnomalyConfigStatus()));

    configContextManager =
        new WebAppEvaluationConfigContextManagerImpl(
            modsecManager,
            modsecRulesRegistry,
            anomalyDetectionConfigManager,
            globalAnomalyConfigStatusManager,
            ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3);
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
            AnomalyConfigScope.newBuilder().setApiScope(AnomalyApiScope.newBuilder().setId(API_ID)))
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
                                        .setBlockingEnabled(true))
                                .addSubRuleConfigs(
                                    AnomalySubRuleConfig.newBuilder()
                                        .setSubRuleId("subRule2")
                                        .setBlockingEnabled(false)))))
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
                                        .setBlockingEnabled(true)))))
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
                                        .setBlockingEnabled(false))
                                .addSubRuleConfigs(
                                    AnomalySubRuleConfig.newBuilder()
                                        .setSubRuleId("subRule2")
                                        .setBlockingEnabled(false)))))
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

    assertEquals(1, result.getSecRuleProcessorConfigsList().size());
    SecRuleProcessorConfig secRuleProcessorConfig = result.getSecRuleProcessorConfigs(0);
    assertEquals(CUSTOMER_SCOPE_CONTEXT, secRuleProcessorConfig.getScopeContext());
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
    assertEquals("sensitiveBlob", rulesContext1.getCrsRulesBlob());

    WebAppEvaluationRulesContext rulesContext2 = result.getWebAppEvaluationRulesContexts(1);
    assertEquals(ENVIRONMENT_SCOPE_CONTEXT, rulesContext2.getScopeContext());
    assertEquals("defaultBlob", rulesContext2.getCrsRulesBlob());
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

    assertEquals(1, result.getSecRuleProcessorConfigsList().size());
    SecRuleProcessorConfig secRuleProcessorConfig = result.getSecRuleProcessorConfigs(0);
    assertEquals(CUSTOMER_SCOPE_CONTEXT, secRuleProcessorConfig.getScopeContext());
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
    assertEquals("sensitiveBlob", rulesContext1.getCrsRulesBlob());

    WebAppEvaluationRulesContext rulesContext2 = result.getWebAppEvaluationRulesContexts(1);
    assertEquals(ENVIRONMENT_SCOPE_CONTEXT, rulesContext2.getScopeContext());
    assertEquals("defaultBlob", rulesContext2.getCrsRulesBlob());
  }
}
