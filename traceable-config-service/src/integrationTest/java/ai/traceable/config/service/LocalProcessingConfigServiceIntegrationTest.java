package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecCrsRulesHandler;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistryImpl;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EventSeverity;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.CreateLocalProcessingRuleRequest;
import ai.traceable.localprocessing.config.service.v1.GetDefaultProtectionModeRequest;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigRequest;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigResponse;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc.LocalProcessingConfigServiceBlockingStub;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRulesServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRulesServiceGrpc.LocalProcessingRulesServiceBlockingStub;
import ai.traceable.localprocessing.config.service.v1.NewLocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ProtectedEndpoint;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import ai.traceable.localprocessing.config.service.v1.ProtectionModeConfig;
import ai.traceable.localprocessing.config.service.v1.SamplingPolicies;
import ai.traceable.localprocessing.config.service.v1.SamplingPolicy;
import ai.traceable.localprocessing.config.service.v1.UpdateDefaultProtectionModeRequest;
import ai.traceable.modsecurity.utils.ModsecRuleUtils;
import java.util.List;
import java.util.Set;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class LocalProcessingConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static LocalProcessingRulesServiceBlockingStub localProcessingRulesStub;
  private static LocalProcessingConfigServiceBlockingStub localProcessingConfigStub;
  private static CustomSignatureConfigServiceBlockingStub customSignatureConfigServiceStub;
  private static final UuidGenerator uuidGenerator = new UuidGenerator();

  @BeforeAll
  static void init() {
    localProcessingRulesStub =
        LocalProcessingRulesServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    localProcessingConfigStub =
        LocalProcessingConfigServiceGrpc.newBlockingStub(channelForExternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    customSignatureConfigServiceStub =
        CustomSignatureConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testLocalProcessingConfigService() {
    NewLocalProcessingRule newLocalProcessingRule1 =
        NewLocalProcessingRule.newBuilder()
            .setUrlPattern("/checkout/*")
            .setHostHeader("abc.com")
            .setProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
            .build();
    NewLocalProcessingRule newLocalProcessingRule2 =
        NewLocalProcessingRule.newBuilder()
            .setUrlPattern("/orders/**")
            .setProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
            .build();
    createRule(newLocalProcessingRule1);
    createRule(newLocalProcessingRule2);

    List<ProtectedEndpoint> protectedEndpoints =
        List.of(
            ProtectedEndpoint.newBuilder()
                .setUrlPattern("/orders/**")
                .setProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
                .build(),
            ProtectedEndpoint.newBuilder()
                .setUrlPattern("/checkout/*")
                .setHostHeader("abc.com")
                .setProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
                .build());
    ProtectionModeConfig protectionModeConfig =
        ProtectionModeConfig.newBuilder()
            .setDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_ADVANCED)
            .addAllProtectedEndpoints(protectedEndpoints)
            .build();

    ProtectionModeConfig expectedConfig =
        ProtectionModeConfig.newBuilder()
            .setDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_ADVANCED)
            .setHash(uuidGenerator.generateId(protectionModeConfig))
            .addAllProtectedEndpoints(protectedEndpoints)
            .build();
    ProtectionModeConfig actualConfig = getConfig();
    assertEquals(expectedConfig, actualConfig);

    // test update default protection mode
    updateDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_ADVANCED);
    protectionModeConfig =
        protectionModeConfig.toBuilder()
            .setDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_ADVANCED)
            .build();
    assertEquals(ProtectionMode.PROTECTION_MODE_ADVANCED, getDefaultProtectionMode());
    expectedConfig =
        expectedConfig.toBuilder()
            .setDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_ADVANCED)
            .setHash(uuidGenerator.generateId(protectionModeConfig))
            .build();
    actualConfig = getConfig();
    assertEquals(expectedConfig, actualConfig);
  }

  @Test
  void testGetModsecConfig_CustomModsec() {
    GetLocalProcessingConfigRequest request =
        GetLocalProcessingConfigRequest.newBuilder()
            .setRegularModsecDetectionRulesHash("regular")
            .setCustomModsecDetectionRulesHash("custom")
            .build();

    GetLocalProcessingConfigResponse response =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> localProcessingConfigStub.getLocalProcessingConfig(request));

    assertTrue(
        response.getCustomModsecDetectionRules().getCustomModsecDetectionRulesBlob().isEmpty());
    assertEquals(uuidGenerator.generateId(""), response.getCustomModsecDetectionRules().getHash());

    createAndGetCustomSignatureRule(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);
    response =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> localProcessingConfigStub.getLocalProcessingConfig(request));
    assertTrue(
        response.getCustomModsecDetectionRules().getCustomModsecDetectionRulesBlob().isEmpty());
    assertEquals(uuidGenerator.generateId(""), response.getCustomModsecDetectionRules().getHash());

    createAndGetCustomSignatureRule(EventType.EVENT_TYPE_NORMAL_DETECTION);
    response =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> localProcessingConfigStub.getLocalProcessingConfig(request));
    assertFalse(
        response.getCustomModsecDetectionRules().getCustomModsecDetectionRulesBlob().isEmpty());
    assertNotEquals(
        uuidGenerator.generateId(""), response.getCustomModsecDetectionRules().getHash());
  }

  @Test
  void testGetModsecConfig_RegularModsec() {
    GetLocalProcessingConfigRequest request =
        GetLocalProcessingConfigRequest.newBuilder()
            .setRegularModsecDetectionRulesHash("regular")
            .setCustomModsecDetectionRulesHash("custom")
            .build();

    GetLocalProcessingConfigResponse response =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> localProcessingConfigStub.getLocalProcessingConfig(request));

    String expectedVal = readModsecRules(Set.of("crs_1030100", "crs_1030110", "crs_1030120"));
    assertEquals(
        expectedVal,
        response.getRegularModsecDetectionRules().getRegularModsecDetectionRulesBlob());
    assertEquals(
        uuidGenerator.generateId(expectedVal), response.getRegularModsecDetectionRules().getHash());
  }

  @Test
  void testGetSamplingPoliciesConfig() {
    GetLocalProcessingConfigRequest request = GetLocalProcessingConfigRequest.getDefaultInstance();
    GetLocalProcessingConfigResponse response =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> localProcessingConfigStub.getLocalProcessingConfig(request));
    SamplingPolicies samplingPolicies = response.getSamplingPolicies();
    assertNotNull(samplingPolicies.getSamplingPoliciesHash());
    assertNotEquals("", samplingPolicies.getSamplingPoliciesHash());
    assertEquals(3, samplingPolicies.getPoliciesCount());

    assertEquals(
        SamplingPolicy.PolicyConfigCase.RATE_LIMITING,
        samplingPolicies.getPolicies(0).getPolicyConfigCase());
    assertEquals("endpoint-rate-limit", samplingPolicies.getPolicies(0).getName());
    assertEquals(
        1000,
        samplingPolicies.getPolicies(0).getRateLimiting().getTraceLimitPerEndpointPerMinute());
    assertEquals(
        10000, samplingPolicies.getPolicies(0).getRateLimiting().getTraceLimitGloballyPerMinute());

    assertEquals(
        SamplingPolicy.PolicyConfigCase.MODSEC_ANOMALY,
        samplingPolicies.getPolicies(1).getPolicyConfigCase());
    assertEquals("modsec-anomaly", samplingPolicies.getPolicies(1).getName());

    assertEquals(
        SamplingPolicy.PolicyConfigCase.SPAN_ATTRIBUTES,
        samplingPolicies.getPolicies(2).getPolicyConfigCase());
    assertEquals("traceableai-blocking-attribute", samplingPolicies.getPolicies(2).getName());
    assertEquals(
        1,
        samplingPolicies
            .getPolicies(2)
            .getSpanAttributes()
            .getAttributesRequiredForSamplingCount());
    assertEquals(
        "traceableai.blocked",
        samplingPolicies
            .getPolicies(2)
            .getSpanAttributes()
            .getAttributesRequiredForSampling(0)
            .getKey());
    assertEquals(
        2,
        samplingPolicies
            .getPolicies(2)
            .getSpanAttributes()
            .getAttributesRequiredForSampling(0)
            .getValuesCount());
    assertEquals(
        "true",
        samplingPolicies
            .getPolicies(2)
            .getSpanAttributes()
            .getAttributesRequiredForSampling(0)
            .getValues(0)
            .getStringValue());
    assertTrue(
        samplingPolicies
            .getPolicies(2)
            .getSpanAttributes()
            .getAttributesRequiredForSampling(0)
            .getValues(1)
            .getBoolValue());

    // test get config with same hash
    String hash = samplingPolicies.getSamplingPoliciesHash();
    GetLocalProcessingConfigRequest request2 =
        GetLocalProcessingConfigRequest.newBuilder().setSamplingPoliciesHash(hash).build();
    response =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> localProcessingConfigStub.getLocalProcessingConfig(request2));
    samplingPolicies = response.getSamplingPolicies();
    assertEquals(hash, samplingPolicies.getSamplingPoliciesHash());
    assertEquals(0, samplingPolicies.getPoliciesCount());
  }

  private void updateDefaultProtectionMode(ProtectionMode protectionMode) {
    UpdateDefaultProtectionModeRequest request =
        UpdateDefaultProtectionModeRequest.newBuilder()
            .setDefaultProtectionMode(protectionMode)
            .build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID, () -> localProcessingRulesStub.updateDefaultProtectionMode(request));
  }

  private ProtectionMode getDefaultProtectionMode() {
    GetDefaultProtectionModeRequest request = GetDefaultProtectionModeRequest.newBuilder().build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> localProcessingRulesStub.getDefaultProtectionMode(request))
        .getDefaultProtectionMode();
  }

  private ProtectionModeConfig getConfig() {
    GetLocalProcessingConfigRequest request = GetLocalProcessingConfigRequest.getDefaultInstance();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> localProcessingConfigStub.getLocalProcessingConfig(request))
        .getProtectionModeConfig();
  }

  private void createRule(NewLocalProcessingRule newLocalProcessingRule) {
    CreateLocalProcessingRuleRequest request =
        CreateLocalProcessingRuleRequest.newBuilder()
            .setNewLocalProcessingRule(newLocalProcessingRule)
            .build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID, () -> localProcessingRulesStub.createLocalProcessingRule(request));
  }

  private String readModsecRules(Set<String> disabledRuleIds) {
    ModsecCrsRulesHandler modsecCrsRulesHandler = new ModsecCrsRulesHandler(new ModsecRuleUtils());
    ModsecRulesRegistry registry =
        new ModsecRulesRegistryImpl(new ConfigConverter(), modsecCrsRulesHandler);
    return registry.getModsecCrsRulesBlob(
        List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE),
        ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE,
        disabledRuleIds,
        false);
  }

  private void createAndGetCustomSignatureRule(EventType eventType) {
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            customSignatureConfigServiceStub.createCustomSignatureRule(
                CreateCustomSignatureRuleRequest.newBuilder()
                    .setName("rule-1")
                    .setDefinition(
                        RuleDefinition.newBuilder()
                            .setClauseGroup(
                                ClauseGroup.newBuilder()
                                    .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                                    .addClauses(
                                        Clause.newBuilder()
                                            .setMatchExpression(
                                                MatchExpression.newBuilder()
                                                    .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                                    .setMatchOperator(
                                                        MatchOperator.MATCH_OPERATOR_CONTAINS)
                                                    .setMatchValue("anomalous")))))
                    .setEffect(
                        RuleEffect.newBuilder()
                            .setEventType(eventType)
                            .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                            .build())
                    .build()));
  }
}
