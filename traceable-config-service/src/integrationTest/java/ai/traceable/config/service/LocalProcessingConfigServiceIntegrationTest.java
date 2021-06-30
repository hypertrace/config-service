package ai.traceable.config.service;

import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_AVAILABLE;
import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_EXHAUSTED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.licensestatus.config.service.v1.LicenseLimit;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceBlockingStub;
import ai.traceable.licensestatus.config.service.v1.UpdateLicenseStatusRequest;
import ai.traceable.localprocessing.config.service.v1.CreateLocalProcessingRuleRequest;
import ai.traceable.localprocessing.config.service.v1.DeleteLocalProcessingRuleRequest;
import ai.traceable.localprocessing.config.service.v1.GetAllLocalProcessingRulesRequest;
import ai.traceable.localprocessing.config.service.v1.GetDefaultProtectionModeRequest;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigRequest;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc.LocalProcessingConfigServiceBlockingStub;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleDetails;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleMetadata;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRulesServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRulesServiceGrpc.LocalProcessingRulesServiceBlockingStub;
import ai.traceable.localprocessing.config.service.v1.NewLocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ProtectedEndpoint;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import ai.traceable.localprocessing.config.service.v1.ProtectionModeConfig;
import ai.traceable.localprocessing.config.service.v1.UpdateDefaultProtectionModeRequest;
import ai.traceable.localprocessing.config.service.v1.UpdateLocalProcessingRuleRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test for LocalProcessingConfigService */
class LocalProcessingConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {

  private static LocalProcessingRulesServiceBlockingStub localProcessingRulesStub;
  private static LocalProcessingConfigServiceBlockingStub localProcessingConfigStub;
  private static LicenseStatusConfigServiceBlockingStub licenseStatusConfigStub;

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
  }

  @Test
  void createReadUpdateDeleteLocalProcessingRules() {
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
    LocalProcessingRuleDetails localProcessingRuleDetails1 = createRule(newLocalProcessingRule1);
    assertEquals(
        buildLocalProcessingRuleDetails(
            newLocalProcessingRule1,
            localProcessingRuleDetails1.getRule().getId(),
            localProcessingRuleDetails1.getMetadata().getCreationTimestamp()),
        localProcessingRuleDetails1);

    LocalProcessingRuleDetails localProcessingRuleDetails2 = createRule(newLocalProcessingRule2);
    assertEquals(List.of(localProcessingRuleDetails2, localProcessingRuleDetails1), getAllRules());

    LocalProcessingRule ruleToUpdate =
        LocalProcessingRule.newBuilder()
            .setId(localProcessingRuleDetails1.getRule().getId())
            .setUrlPattern("/checkout/v1/*")
            .setHostHeader("abc.com")
            .setProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
            .build();
    LocalProcessingRuleDetails updatedRuleDetails = updateRule(ruleToUpdate);
    assertEquals(
        buildLocalProcessingRuleDetails(
            ruleToUpdate, localProcessingRuleDetails1.getMetadata().getCreationTimestamp()),
        updatedRuleDetails);
    assertEquals(List.of(localProcessingRuleDetails2, updatedRuleDetails), getAllRules());

    deleteRule(localProcessingRuleDetails2.getRule().getId());
    assertEquals(List.of(updatedRuleDetails), getAllRules());
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

  private LocalProcessingRuleDetails buildLocalProcessingRuleDetails(
      NewLocalProcessingRule newLocalProcessingRule, String id, long creationTimestamp) {
    LocalProcessingRule localProcessingRule =
        LocalProcessingRule.newBuilder()
            .setId(id)
            .setUrlPattern(newLocalProcessingRule.getUrlPattern())
            .setHostHeader(newLocalProcessingRule.getHostHeader())
            .setProtectionMode(newLocalProcessingRule.getProtectionMode())
            .build();
    return buildLocalProcessingRuleDetails(localProcessingRule, creationTimestamp);
  }

  private LocalProcessingRuleDetails buildLocalProcessingRuleDetails(
      LocalProcessingRule localProcessingRule, long creationTimestamp) {
    return LocalProcessingRuleDetails.newBuilder()
        .setRule(localProcessingRule)
        .setMetadata(
            LocalProcessingRuleMetadata.newBuilder()
                .setCreationTimestamp(creationTimestamp)
                .build())
        .build();
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

  private LocalProcessingRuleDetails createRule(NewLocalProcessingRule newLocalProcessingRule) {
    CreateLocalProcessingRuleRequest request =
        CreateLocalProcessingRuleRequest.newBuilder()
            .setNewLocalProcessingRule(newLocalProcessingRule)
            .build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> localProcessingRulesStub.createLocalProcessingRule(request))
        .getLocalProcessingRuleDetails();
  }

  private LocalProcessingRuleDetails updateRule(LocalProcessingRule localProcessingRule) {
    UpdateLocalProcessingRuleRequest request =
        UpdateLocalProcessingRuleRequest.newBuilder()
            .setLocalProcessingRule(localProcessingRule)
            .build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> localProcessingRulesStub.updateLocalProcessingRule(request))
        .getLocalProcessingRuleDetails();
  }

  private void deleteRule(String localProcessingRuleId) {
    DeleteLocalProcessingRuleRequest request =
        DeleteLocalProcessingRuleRequest.newBuilder()
            .setLocalProcessingRuleId(localProcessingRuleId)
            .build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID, () -> localProcessingRulesStub.deleteLocalProcessingRule(request));
  }

  private List<LocalProcessingRuleDetails> getAllRules() {
    GetAllLocalProcessingRulesRequest request =
        GetAllLocalProcessingRulesRequest.getDefaultInstance();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> localProcessingRulesStub.getAllLocalProcessingRules(request))
        .getLocalProcessingRulesDetailsList();
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

  // TODO: Write integration test for customRules and regularModsecRules
}
