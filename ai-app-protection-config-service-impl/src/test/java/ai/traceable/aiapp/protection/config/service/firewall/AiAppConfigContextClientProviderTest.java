package ai.traceable.aiapp.protection.config.service.firewall;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.aiapp.protection.config.service.firewall.cache.AiAppConfigContextClientProvider;
import ai.traceable.aiapp.protection.config.service.firewall.cache.ProtectionEngineDataTypeTranslator;
import ai.traceable.aiapp.protection.config.service.firewall.converter.PiiRuleToCustomSignatureConfigConverter;
import ai.traceable.aiapp.protection.config.service.v1.AiAppConfigServiceGrpc;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiAppRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppSubRule;
import ai.traceable.aiapp.protection.config.service.v1.DatatypeCondition;
import ai.traceable.aiapp.protection.config.service.v1.EnvironmentScope;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppEvaluationConfigContextRequest;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppRulesResponse;
import ai.traceable.aiapp.protection.config.service.v1.PiiDetectedInPromptRuleData;
import ai.traceable.aiapp.protection.config.service.v1.RuleEvaluationPoint;
import ai.traceable.aiapp.protection.config.service.v1.RuleScope;
import ai.traceable.aiapp.protection.config.service.v1.TenantScope;
import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.detector.*;
import ai.traceable.data.classification.cache.client.DataClassificationClient;
import ai.traceable.data.classification.cache.info.DataClassificationInfo;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.protection.engine.config.aifirewall.v1.AiFirewallConfigContext;
import ai.traceable.protection.engine.config.aifirewall.v1.AiFirewallScopedConfigContext;
import ai.traceable.protection.engine.config.aifirewall.v1.SecRulesEvaluationConfig;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRulesContext;
import ai.traceable.protection.rules.aiapp.v1.AiAppRules;
import ai.traceable.protection.rules.aiapp.v1.AiAppRulesProvider;
import ai.traceable.protection.rules.aiapp.v1.AiAppThreatRule;
import ai.traceable.protection.rules.aiapp.v1.AiAppThreatRuleEvaluation;
import ai.traceable.protection.rules.aiapp.v1.SecRuleEvaluation;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AiAppConfigContextClientProviderTest {

  private static final String TENANT_ID = "test-tenant";
  private static final String SEC_RULE_ID_1 = "rule_sec-rule-1";
  private static final String SANITISED_SEC_RULE_ID_1 = "sec-rule-1";
  private static final String SEC_RULE_ID_2 = "rule_sec-rule-2";
  private static final String SANITISED_SEC_RULE_ID_2 = "sec-rule-2";
  private static final String NON_SEC_RULE_ID = "rule_non-sec-rule-1";
  private static final String SEC_RULES_BLOB = "test-sec-rules-blob";

  private static final AnomalyConfigScope CUSTOMER_SCOPE =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();

  private AiAppConfigContextClientProvider provider;
  private AnomalyDetectionConfigManager anomalyDetectionConfigManager;
  private AiAppRulesProvider aiAppRulesProvider;
  private DataClassificationClient dataClassificationClient;
  private AiAppConfigServiceGrpc.AiAppConfigServiceBlockingStub aiAppConfigService;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    anomalyDetectionConfigManager = mock(AnomalyDetectionConfigManager.class);
    aiAppRulesProvider = mock(AiAppRulesProvider.class);
    CachedServiceMappingProvider cachedServiceMappingProvider =
        mock(CachedServiceMappingProvider.class);
    CachedApiMappingProvider cachedApiMappingProvider = mock(CachedApiMappingProvider.class);
    dataClassificationClient = mock(DataClassificationClient.class);
    aiAppConfigService = mock(AiAppConfigServiceGrpc.AiAppConfigServiceBlockingStub.class);

    DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub
        dataClassificationConfigServiceStub =
            mock(
                DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub
                    .class);
    ProtectionEngineDataTypeTranslator protectionEngineDataTypeTranslator =
        new ProtectionEngineDataTypeTranslator();

    provider =
        new AiAppConfigContextClientProvider(
            anomalyDetectionConfigManager,
            aiAppRulesProvider,
            cachedServiceMappingProvider,
            cachedApiMappingProvider,
            dataClassificationConfigServiceStub,
            protectionEngineDataTypeTranslator,
            dataClassificationClient,
            aiAppConfigService,
            new PiiRuleToCustomSignatureConfigConverter());

    requestContext = RequestContext.forTenantId(TENANT_ID);

    // Default mocks for PII rule fetching (no PII rules by default)
    when(aiAppConfigService.getAiAppRules(any()))
        .thenReturn(GetAiAppRulesResponse.getDefaultInstance());
    when(dataClassificationClient.getDataClassificationInfo(any(RequestContext.class)))
        .thenReturn(new DataClassificationInfo(Map.of(), Map.of()));
  }

  @Test
  void testEmptyConfig_ReturnsDefaultContext() {
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of());
    when(aiAppRulesProvider.getAiAppRules())
        .thenReturn(AiAppRules.newBuilder().setAiAppRulesBlob(SEC_RULES_BLOB).build());

    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    assertEquals(SEC_RULES_BLOB, result.getSecRulesBlob());
    assertTrue(result.getScopedConfigContextsList().isEmpty());
  }

  @Test
  void testDisabledSecRule_AddsToDisabledSecRuleIds() {
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(
            List.of(
                buildScopedConfig(
                    CUSTOMER_SCOPE,
                    Map.of(
                        SEC_RULE_ID_1,
                        buildSubRuleConfig(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE)))));
    when(aiAppRulesProvider.getAiAppRules())
        .thenReturn(buildAiAppRulesWithSecRules(List.of(SEC_RULE_ID_1)));

    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    assertEquals(1, result.getScopedConfigContextsCount());
    AiFirewallScopedConfigContext scopedCtx = result.getScopedConfigContexts(0);
    SecRulesEvaluationConfig secConfig = scopedCtx.getSecRulesEvaluationConfig();
    assertEquals(List.of(SANITISED_SEC_RULE_ID_1), secConfig.getDisabledSecRuleIdsList());
  }

  @Test
  void testEnabledSecRule_NotInDisabledSecRuleIds() {
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(
            List.of(
                buildScopedConfig(
                    CUSTOMER_SCOPE,
                    Map.of(
                        SEC_RULE_ID_1,
                        buildSubRuleConfig(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK)))));
    when(aiAppRulesProvider.getAiAppRules())
        .thenReturn(buildAiAppRulesWithSecRules(List.of(SEC_RULE_ID_1)));

    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    assertEquals(1, result.getScopedConfigContextsCount());
    AiFirewallScopedConfigContext scopedCtx = result.getScopedConfigContexts(0);
    assertTrue(scopedCtx.getSecRulesEvaluationConfig().getDisabledSecRuleIdsList().isEmpty());
  }

  @Test
  void testNonSecRule_DisabledNotAddedToDisabledSecRuleIds() {
    // A rule that is NOT sec-rule-evaluated should NOT appear in disabledSecRuleIds even if
    // disabled
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(
            List.of(
                buildScopedConfig(
                    CUSTOMER_SCOPE,
                    Map.of(
                        NON_SEC_RULE_ID,
                        buildSubRuleConfig(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE)))));
    when(aiAppRulesProvider.getAiAppRules()).thenReturn(buildAiAppRulesWithSecRules(List.of()));

    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    assertEquals(1, result.getScopedConfigContextsCount());
    AiFirewallScopedConfigContext scopedCtx = result.getScopedConfigContexts(0);
    assertTrue(scopedCtx.getSecRulesEvaluationConfig().getDisabledSecRuleIdsList().isEmpty());
  }

  @Test
  void testMixedRules_OnlySecRuleDisabledAdded() {
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(
            List.of(
                buildScopedConfig(
                    CUSTOMER_SCOPE,
                    Map.of(
                        SEC_RULE_ID_1,
                        buildSubRuleConfig(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK),
                        SEC_RULE_ID_2,
                        buildSubRuleConfig(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE),
                        NON_SEC_RULE_ID,
                        buildSubRuleConfig(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE)))));
    when(aiAppRulesProvider.getAiAppRules())
        .thenReturn(buildAiAppRulesWithSecRules(List.of(SEC_RULE_ID_1, SEC_RULE_ID_2)));

    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    assertEquals(1, result.getScopedConfigContextsCount());
    AiFirewallScopedConfigContext scopedCtx = result.getScopedConfigContexts(0);

    // SEC_RULE_ID_1 is enabled (BLOCK) → NOT in disabled list
    // SEC_RULE_ID_2 is disabled (DISABLE action, not BLOCK for edge) → in disabled list
    // NON_SEC_RULE_ID is disabled but not sec-rule-evaluated → NOT in disabled list
    SecRulesEvaluationConfig secConfig = scopedCtx.getSecRulesEvaluationConfig();
    assertEquals(List.of(SANITISED_SEC_RULE_ID_2), secConfig.getDisabledSecRuleIdsList());
  }

  @Test
  void testPlatformEvaluationPoint_DisabledMeansActionDisable() {
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(
            List.of(
                buildScopedConfig(
                    CUSTOMER_SCOPE,
                    Map.of(
                        SEC_RULE_ID_1,
                        buildSubRuleConfig(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE),
                        SEC_RULE_ID_2,
                        buildSubRuleConfig(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR)))));
    when(aiAppRulesProvider.getAiAppRules())
        .thenReturn(buildAiAppRulesWithSecRules(List.of(SEC_RULE_ID_1, SEC_RULE_ID_2)));

    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    assertEquals(1, result.getScopedConfigContextsCount());
    AiFirewallScopedConfigContext scopedCtx = result.getScopedConfigContexts(0);

    // For PLATFORM: disabled = action is DISABLE
    // SEC_RULE_ID_1 is DISABLE → disabled → in disabled list
    // SEC_RULE_ID_2 is MONITOR → enabled → NOT in disabled list
    assertEquals(
        List.of(SANITISED_SEC_RULE_ID_1),
        scopedCtx.getSecRulesEvaluationConfig().getDisabledSecRuleIdsList());
  }

  @Test
  void testConfigDisabled_AllRulesDisabled() {
    ScopedAnomalyDetectionConfig scopedConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(CUSTOMER_SCOPE)
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
                    .setGenAiAnomalyDetectionConfig(
                        GenAiAnomalyDetectionConfig.newBuilder()
                            .setSubRuleConfigs(
                                AnomalySubRuleConfigMap.newBuilder()
                                    .putAllSubRuleConfigs(
                                        Map.of(
                                            SEC_RULE_ID_1,
                                            buildSubRuleConfig(
                                                AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK))))))
            .build();

    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(scopedConfig));
    when(aiAppRulesProvider.getAiAppRules())
        .thenReturn(buildAiAppRulesWithSecRules(List.of(SEC_RULE_ID_1)));

    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    assertEquals(1, result.getScopedConfigContextsCount());
    AiFirewallScopedConfigContext scopedCtx = result.getScopedConfigContexts(0);

    // Config is disabled → all sec rules are disabled regardless of action
    assertEquals(
        List.of(SANITISED_SEC_RULE_ID_1),
        scopedCtx.getSecRulesEvaluationConfig().getDisabledSecRuleIdsList());
  }

  @Test
  void testSecRulesBlobPassedThrough() {
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of());
    when(aiAppRulesProvider.getAiAppRules())
        .thenReturn(AiAppRules.newBuilder().setAiAppRulesBlob("custom-blob-content").build());

    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    assertEquals("custom-blob-content", result.getSecRulesBlob());
  }

  @Test
  void testPiiRules_PopulateCustomSignatureConfigContext() {
    setupEmptyAnomalyConfig();

    when(aiAppConfigService.getAiAppRules(any()))
        .thenReturn(
            GetAiAppRulesResponse.newBuilder()
                .addAiAppRules(
                    AiAppRule.newBuilder()
                        .setRuleId("rule-group-1")
                        .addAiAppSubRules(
                            AiAppSubRule.newBuilder()
                                .setCustomRule(buildPiiCustomRule("pii-rule-1", "PII Rule"))))
                .build());

    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    assertTrue(result.hasCustomSignatureConfigContext());
    assertTrue(result.getCustomSignatureConfigContext().getRuleContextsCount() >= 1);
    assertTrue(
        result.getCustomSignatureConfigContext().getRuleContextsList().stream()
            .flatMap(ctx -> ctx.getRuleConfigsList().stream())
            .anyMatch(rc -> rc.getId().equals("pii-rule-1")));
  }

  @Test
  void testPiiRules_NotSetForPlatformEvaluationPoint() {
    setupEmptyAnomalyConfig();

    when(aiAppConfigService.getAiAppRules(any()))
        .thenReturn(
            GetAiAppRulesResponse.newBuilder()
                .addAiAppRules(
                    AiAppRule.newBuilder()
                        .setRuleId("rule-group-1")
                        .addAiAppSubRules(
                            AiAppSubRule.newBuilder()
                                .setCustomRule(buildPiiCustomRule("pii-rule-1", "PII Rule"))))
                .build());

    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    // PII rules should NOT be present for PLATFORM evaluation point
    assertEquals(
        0,
        result.getCustomSignatureConfigContext().getRuleContextsList().stream()
            .mapToInt(ctx -> ctx.getRuleConfigsCount())
            .sum());
  }

  @Test
  void testNoPiiRules_NoCustomSignatureConfigContext() {
    setupEmptyAnomalyConfig();
    when(aiAppConfigService.getAiAppRules(any()))
        .thenReturn(GetAiAppRulesResponse.getDefaultInstance());

    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    assertEquals(0, result.getCustomSignatureConfigContext().getRuleContextsCount());
  }

  @Test
  void testScopeFiltering_EnvScopedRuleExcludedForDifferentEnv() {
    setupEmptyAnomalyConfig();

    // Rule scoped to env-2
    AiAppCustomRule envRule =
        buildPiiCustomRuleWithScope(
            "pii-env2",
            "Env2 Rule",
            RuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-2"))
                .build());

    when(aiAppConfigService.getAiAppRules(any()))
        .thenReturn(
            GetAiAppRulesResponse.newBuilder()
                .addAiAppRules(
                    AiAppRule.newBuilder()
                        .setRuleId("rg-1")
                        .addAiAppSubRules(AiAppSubRule.newBuilder().setCustomRule(envRule)))
                .build());

    // Request for env-1
    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-1")))
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    // env-2 rule should be excluded
    assertEquals(
        0,
        result.getCustomSignatureConfigContext().getRuleContextsList().stream()
            .mapToInt(ctx -> ctx.getRuleConfigsCount())
            .sum());
  }

  @Test
  void testScopeFiltering_TenantScopedRuleIncludedForEnvRequest() {
    setupEmptyAnomalyConfig();

    // Tenant-scoped rule
    AiAppCustomRule tenantRule =
        buildPiiCustomRuleWithScope(
            "pii-tenant",
            "Tenant Rule",
            RuleScope.newBuilder().setTenantScope(TenantScope.getDefaultInstance()).build());

    when(aiAppConfigService.getAiAppRules(any()))
        .thenReturn(
            GetAiAppRulesResponse.newBuilder()
                .addAiAppRules(
                    AiAppRule.newBuilder()
                        .setRuleId("rg-1")
                        .addAiAppSubRules(AiAppSubRule.newBuilder().setCustomRule(tenantRule)))
                .build());

    // Request for env-1
    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-1")))
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    // Tenant-scoped rule should be included
    assertTrue(
        result.getCustomSignatureConfigContext().getRuleContextsList().stream()
            .flatMap(ctx -> ctx.getRuleConfigsList().stream())
            .anyMatch(rc -> rc.getId().equals("pii-tenant")));
  }

  @Test
  void testScopeFiltering_MatchingEnvRuleIncluded() {
    setupEmptyAnomalyConfig();

    // Rule scoped to env-1
    AiAppCustomRule envRule =
        buildPiiCustomRuleWithScope(
            "pii-env1",
            "Env1 Rule",
            RuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-1"))
                .build());

    when(aiAppConfigService.getAiAppRules(any()))
        .thenReturn(
            GetAiAppRulesResponse.newBuilder()
                .addAiAppRules(
                    AiAppRule.newBuilder()
                        .setRuleId("rg-1")
                        .addAiAppSubRules(AiAppSubRule.newBuilder().setCustomRule(envRule)))
                .build());

    // Request for env-1
    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-1")))
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    assertTrue(
        result.getCustomSignatureConfigContext().getRuleContextsList().stream()
            .flatMap(ctx -> ctx.getRuleConfigsList().stream())
            .anyMatch(rc -> rc.getId().equals("pii-env1")));
  }

  @Test
  void testScopeFiltering_MixedScopes_OnlyMatchingReturned() {
    setupEmptyAnomalyConfig();

    AiAppCustomRule tenantRule =
        buildPiiCustomRuleWithScope(
            "pii-tenant",
            "Tenant Rule",
            RuleScope.newBuilder().setTenantScope(TenantScope.getDefaultInstance()).build());
    AiAppCustomRule env1Rule =
        buildPiiCustomRuleWithScope(
            "pii-env1",
            "Env1 Rule",
            RuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-1"))
                .build());
    AiAppCustomRule env2Rule =
        buildPiiCustomRuleWithScope(
            "pii-env2",
            "Env2 Rule",
            RuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-2"))
                .build());

    when(aiAppConfigService.getAiAppRules(any()))
        .thenReturn(
            GetAiAppRulesResponse.newBuilder()
                .addAiAppRules(
                    AiAppRule.newBuilder()
                        .setRuleId("rg-1")
                        .addAiAppSubRules(AiAppSubRule.newBuilder().setCustomRule(tenantRule))
                        .addAiAppSubRules(AiAppSubRule.newBuilder().setCustomRule(env1Rule))
                        .addAiAppSubRules(AiAppSubRule.newBuilder().setCustomRule(env2Rule)))
                .build());

    // Request for env-1 → tenant rule + env-1 rule included, env-2 rule excluded
    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-1")))
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    List<String> ruleIds =
        result.getCustomSignatureConfigContext().getRuleContextsList().stream()
            .flatMap(ctx -> ctx.getRuleConfigsList().stream())
            .map(rc -> rc.getId())
            .collect(java.util.stream.Collectors.toList());

    assertTrue(ruleIds.contains("pii-tenant"), "Tenant-scoped rule should be included");
    assertTrue(ruleIds.contains("pii-env1"), "Matching env-scoped rule should be included");
    assertEquals(2, ruleIds.size(), "Only tenant + matching env rules should be returned");
  }

  @Test
  void testScopeFiltering_TenantRequestIncludesAllRules() {
    setupEmptyAnomalyConfig();

    AiAppCustomRule tenantRule =
        buildPiiCustomRuleWithScope(
            "pii-tenant",
            "Tenant Rule",
            RuleScope.newBuilder().setTenantScope(TenantScope.getDefaultInstance()).build());
    AiAppCustomRule env1Rule =
        buildPiiCustomRuleWithScope(
            "pii-env1",
            "Env1 Rule",
            RuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-1"))
                .build());

    when(aiAppConfigService.getAiAppRules(any()))
        .thenReturn(
            GetAiAppRulesResponse.newBuilder()
                .addAiAppRules(
                    AiAppRule.newBuilder()
                        .setRuleId("rg-1")
                        .addAiAppSubRules(AiAppSubRule.newBuilder().setCustomRule(tenantRule))
                        .addAiAppSubRules(AiAppSubRule.newBuilder().setCustomRule(env1Rule)))
                .build());

    // Tenant-scoped request → all rules match
    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setRuleScope(RuleScope.newBuilder().setTenantScope(TenantScope.getDefaultInstance()))
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    List<String> ruleIds =
        result.getCustomSignatureConfigContext().getRuleContextsList().stream()
            .flatMap(ctx -> ctx.getRuleConfigsList().stream())
            .map(rc -> rc.getId())
            .collect(java.util.stream.Collectors.toList());

    assertTrue(ruleIds.contains("pii-tenant"));
    assertTrue(ruleIds.contains("pii-env1"));
    assertEquals(2, ruleIds.size());
  }

  @Test
  void testDisabledPiiRule_IsExcluded() {
    setupEmptyAnomalyConfig();

    // Disabled PII rule
    AiAppCustomRule disabledRule =
        AiAppCustomRule.newBuilder()
            .setRuleId("pii-disabled")
            .setRuleData(
                AiAppCustomRuleData.newBuilder()
                    .setRuleName("Disabled Rule")
                    .setEnabled(false)
                    .setPiiDetectedInPromptRuleData(
                        PiiDetectedInPromptRuleData.newBuilder()
                            .setDatatypeCondition(
                                DatatypeCondition.newBuilder().addDatatypeIds("datatype-1"))))
            .build();

    when(aiAppConfigService.getAiAppRules(any()))
        .thenReturn(
            GetAiAppRulesResponse.newBuilder()
                .addAiAppRules(
                    AiAppRule.newBuilder()
                        .setRuleId("rg-1")
                        .addAiAppSubRules(AiAppSubRule.newBuilder().setCustomRule(disabledRule)))
                .build());

    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    assertEquals(0, result.getCustomSignatureConfigContext().getRuleContextsCount());
  }

  @Test
  void testDisabledPiiDetectionInPromptDetectionConfig_SkipsPiiRules() {
    ScopedAnomalyDetectionConfig disabledConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(CUSTOMER_SCOPE)
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
                    .setGenAiAnomalyDetectionConfig(
                        GenAiAnomalyDetectionConfig.newBuilder()
                            .setPiiDetectedInPrompt(
                                PiiDetectedInPromptAnomalyDetectionConfig.getDefaultInstance())))
            .build();

    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of(disabledConfig));
    when(aiAppRulesProvider.getAiAppRules())
        .thenReturn(AiAppRules.newBuilder().setAiAppRulesBlob(SEC_RULES_BLOB).build());

    when(aiAppConfigService.getAiAppRules(any()))
        .thenReturn(
            GetAiAppRulesResponse.newBuilder()
                .addAiAppRules(
                    AiAppRule.newBuilder()
                        .setRuleId("rule-group-1")
                        .addAiAppSubRules(
                            AiAppSubRule.newBuilder()
                                .setCustomRule(buildPiiCustomRule("pii-rule-1", "PII Rule"))))
                .build());

    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();

    AiFirewallConfigContext result = provider.getAiFirewallConfigContext(requestContext, request);

    // PII rules must be absent when detection config is disabled
    assertEquals(
        0,
        result.getCustomSignatureConfigContext().getRuleContextsList().stream()
            .mapToInt(CustomSignatureRulesContext::getRuleConfigsCount)
            .sum(),
        "PII rule configs must be skipped when detection config is disabled");
  }

  private ScopedAnomalyDetectionConfig buildScopedConfig(
      AnomalyConfigScope scope, Map<String, AnomalySubRuleConfig> subRuleConfigMap) {
    return ScopedAnomalyDetectionConfig.newBuilder()
        .setConfigScope(scope)
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setGenAiAnomalyDetectionConfig(
                    GenAiAnomalyDetectionConfig.newBuilder()
                        .setSubRuleConfigs(
                            AnomalySubRuleConfigMap.newBuilder()
                                .putAllSubRuleConfigs(subRuleConfigMap))))
        .build();
  }

  private AnomalySubRuleConfig buildSubRuleConfig(AnomalyRuleAction action) {
    return AnomalySubRuleConfig.newBuilder().setAnomalyRuleAction(action).build();
  }

  private AiAppRules buildAiAppRulesWithSecRules(List<String> secRuleIds) {
    AiAppRules.Builder builder = AiAppRules.newBuilder().setAiAppRulesBlob(SEC_RULES_BLOB);
    for (String ruleId : secRuleIds) {
      builder.addThreatRules(
          AiAppThreatRule.newBuilder()
              .setRuleId(ruleId)
              .setRuleEvaluation(
                  AiAppThreatRuleEvaluation.newBuilder()
                      .setSecRuleEvaluation(SecRuleEvaluation.getDefaultInstance()))
              .build());
    }
    return builder.build();
  }

  private void setupEmptyAnomalyConfig() {
    when(anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            any(RequestContext.class), any()))
        .thenReturn(List.of());
    when(aiAppRulesProvider.getAiAppRules())
        .thenReturn(AiAppRules.newBuilder().setAiAppRulesBlob(SEC_RULES_BLOB).build());
  }

  private AiAppCustomRule buildPiiCustomRule(String ruleId, String ruleName) {
    return AiAppCustomRule.newBuilder()
        .setRuleId(ruleId)
        .setRuleData(
            AiAppCustomRuleData.newBuilder()
                .setRuleName(ruleName)
                .setDescription("Test PII rule")
                .setEnabled(true)
                .setPiiDetectedInPromptRuleData(
                    PiiDetectedInPromptRuleData.newBuilder()
                        .setDatatypeCondition(
                            DatatypeCondition.newBuilder().addDatatypeIds("datatype-1"))))
        .build();
  }

  private AiAppCustomRule buildPiiCustomRuleWithScope(
      String ruleId, String ruleName, RuleScope ruleScope) {
    return AiAppCustomRule.newBuilder()
        .setRuleId(ruleId)
        .setRuleData(
            AiAppCustomRuleData.newBuilder()
                .setRuleName(ruleName)
                .setDescription("Test PII rule")
                .setEnabled(true)
                .setRuleScope(ruleScope)
                .setPiiDetectedInPromptRuleData(
                    PiiDetectedInPromptRuleData.newBuilder()
                        .setDatatypeCondition(
                            DatatypeCondition.newBuilder().addDatatypeIds("datatype-1"))))
        .build();
  }
}
