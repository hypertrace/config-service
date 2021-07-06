package ai.traceable.config.service;

import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_AVAILABLE;
import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_EXHAUSTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecCrsRulesHandler;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRuleUtils;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistryImpl;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
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
import ai.traceable.licensestatus.config.service.v1.LicenseLimit;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceBlockingStub;
import ai.traceable.licensestatus.config.service.v1.UpdateLicenseStatusRequest;
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
import ai.traceable.localprocessing.config.service.v1.UpdateDefaultProtectionModeRequest;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class LocalProcessingConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static LocalProcessingRulesServiceBlockingStub localProcessingRulesStub;
  private static LocalProcessingConfigServiceBlockingStub localProcessingConfigStub;
  private static LicenseStatusConfigServiceBlockingStub licenseStatusConfigStub;
  private static CustomSignatureConfigServiceBlockingStub customSignatureConfigServiceStub;
  private static final UuidGenerator uuidGenerator = new UuidGenerator();

  @BeforeAll
  static void init() {
    localProcessingRulesStub =
        LocalProcessingRulesServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    localProcessingConfigStub =
        LocalProcessingConfigServiceGrpc.newBlockingStub(managedChannelForExternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    licenseStatusConfigStub =
        LicenseStatusConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    customSignatureConfigServiceStub =
        CustomSignatureConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testLocalProcessingConfigService() {
    setLicenseStatus(LICENSE_LIMIT_AVAILABLE);
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

    ProtectionModeConfig expectedConfig =
        ProtectionModeConfig.newBuilder()
            .setDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_ADVANCED)
            .addProtectedEndpoints(
                ProtectedEndpoint.newBuilder()
                    .setUrlPattern("/orders/**")
                    .setProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
                    .build())
            .addProtectedEndpoints(
                ProtectedEndpoint.newBuilder()
                    .setUrlPattern("/checkout/*")
                    .setHostHeader("abc.com")
                    .setProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
                    .build())
            .build();
    ProtectionModeConfig actualConfig = getConfig();
    assertEquals(expectedConfig, actualConfig);

    // test default protection mode
    updateDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_CORE);
    assertEquals(ProtectionMode.PROTECTION_MODE_CORE, getDefaultProtectionMode());
    expectedConfig =
        expectedConfig.toBuilder()
            .setDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
            .build();
    actualConfig = getConfig();
    assertEquals(expectedConfig, actualConfig);
  }

  @Test
  void testLocalProcessingConfigServiceWithLimitExhausted() {
    setLicenseStatus(LICENSE_LIMIT_EXHAUSTED);
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

    ProtectionModeConfig expectedConfig =
        ProtectionModeConfig.newBuilder()
            .setDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
            .build();
    ProtectionModeConfig actualConfig = getConfig();
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

    String expectedVal = readModsecRules();
    assertEquals(
        expectedVal,
        response.getRegularModsecDetectionRules().getRegularModsecDetectionRulesBlob());
    assertEquals(
        uuidGenerator.generateId(expectedVal), response.getRegularModsecDetectionRules().getHash());
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

  private void setLicenseStatus(LicenseLimit licenseLimit) {
    UpdateLicenseStatusRequest request =
        UpdateLicenseStatusRequest.newBuilder()
            .setLicenseStatus(
                LicenseStatus.newBuilder().setTracesLicenseLimit(licenseLimit).build())
            .build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID, () -> licenseStatusConfigStub.updateLicenseStatus(request));
  }

  private void createRule(NewLocalProcessingRule newLocalProcessingRule) {
    CreateLocalProcessingRuleRequest request =
        CreateLocalProcessingRuleRequest.newBuilder()
            .setNewLocalProcessingRule(newLocalProcessingRule)
            .build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID, () -> localProcessingRulesStub.createLocalProcessingRule(request));
  }

  private String readModsecRules() {
    ModsecCrsRulesHandler modsecCrsRulesHandler = new ModsecCrsRulesHandler(new ModsecRuleUtils());
    ModsecRulesRegistry registry =
        new ModsecRulesRegistryImpl(new ConfigConverter(), modsecCrsRulesHandler);
    return registry.getModsecCrsRulesBlob(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR);
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
