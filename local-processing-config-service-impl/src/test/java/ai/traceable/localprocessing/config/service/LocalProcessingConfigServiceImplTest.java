package ai.traceable.localprocessing.config.service;

import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_AVAILABLE;
import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_EXHAUSTED;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.DEFAULT_PROTECTION_MODE;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusRequest;
import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusResponse;
import ai.traceable.licensestatus.config.service.v1.LicenseLimit;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceBlockingStub;
import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinator;
import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinatorImpl;
import ai.traceable.localprocessing.config.service.customsignature.CustomModsecDetectionManager;
import ai.traceable.localprocessing.config.service.regularmodsec.RegularModsecDetectionManager;
import ai.traceable.localprocessing.config.service.ruleservice.LocalProcessingRulesServiceImpl;
import ai.traceable.localprocessing.config.service.v1.CreateLocalProcessingRuleRequest;
import ai.traceable.localprocessing.config.service.v1.CustomModsecDetectionRules;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigRequest;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigResponse;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc.LocalProcessingConfigServiceBlockingStub;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleDetails;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRulesServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRulesServiceGrpc.LocalProcessingRulesServiceBlockingStub;
import ai.traceable.localprocessing.config.service.v1.NewLocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ProtectedEndpoint;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import ai.traceable.localprocessing.config.service.v1.ProtectionModeConfig;
import ai.traceable.localprocessing.config.service.v1.RegularModsecDetectionRules;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class LocalProcessingConfigServiceImplTest {

  LocalProcessingConfigServiceBlockingStub localProcessingConfigStub;
  LocalProcessingRulesServiceBlockingStub localProcessingRulesStub;
  LicenseStatusConfigServiceBlockingStub licenseStatusConfigStub;
  MockGenericConfigService mockGenericConfigService;
  LicenseStatus licenseStatus;
  CustomModsecDetectionManager customModsecDetectionManager;
  RegularModsecDetectionManager regularModsecDetectionManager;

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGet().mockGetAll();

    Map<String, Map> configMap = new HashMap<>();
    configMap.put(
        LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG,
        Map.of(DEFAULT_PROTECTION_MODE, ProtectionMode.PROTECTION_MODE_ADVANCED.name()));
    configMap.put(
        "license.status.config.service",
        Map.of("default.license.limit", "LICENSE_LIMIT_AVAILABLE"));

    Config config = ConfigFactory.parseMap(configMap);
    ManagedChannel channel = (ManagedChannel) mockGenericConfigService.channel();

    customModsecDetectionManager = mock(CustomModsecDetectionManager.class);
    regularModsecDetectionManager = mock(RegularModsecDetectionManager.class);

    ConfigServiceCoordinator configServiceCoordinator =
        new ConfigServiceCoordinatorImpl(channel, new LocalProcessingConfigServiceConfig(config));
    mockGenericConfigService
        .addService(
            new LocalProcessingConfigServiceImpl(
                channel,
                configServiceCoordinator,
                customModsecDetectionManager,
                regularModsecDetectionManager))
        .addService(new LocalProcessingRulesServiceImpl(configServiceCoordinator))
        .addService(new MockLicenseStatusConfigService())
        .start();

    localProcessingConfigStub = LocalProcessingConfigServiceGrpc.newBlockingStub(channel);
    localProcessingRulesStub = LocalProcessingRulesServiceGrpc.newBlockingStub(channel);
    licenseStatusConfigStub = LicenseStatusConfigServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  @DisplayName("Test get local processing config modsec rules part")
  void getLocalProcessingConfig_modsecRules() {
    CustomModsecDetectionRules expectedCustomModsecDetectionRules =
        CustomModsecDetectionRules.newBuilder()
            .setHash("Custom")
            .setCustomModsecDetectionRulesBlob("Custom rules")
            .build();

    RegularModsecDetectionRules expectedRegularModsecDetectionRules =
        RegularModsecDetectionRules.newBuilder()
            .setHash("Regular")
            .setRegularModsecDetectionRulesBlob("Regular rules")
            .build();

    when(customModsecDetectionManager.getEnabledRules("Custom"))
        .thenReturn(expectedCustomModsecDetectionRules);
    when(regularModsecDetectionManager.getDetectionRules("Regular"))
        .thenReturn(expectedRegularModsecDetectionRules);

    setLicenseStatus(LICENSE_LIMIT_AVAILABLE);
    createLocalProcessingRule("/checkout/*", "abc.com", ProtectionMode.PROTECTION_MODE_CORE);
    createLocalProcessingRule("/orders/**", "xyz.com", ProtectionMode.PROTECTION_MODE_ADVANCED);

    GetLocalProcessingConfigResponse response =
        localProcessingConfigStub.getLocalProcessingConfig(
            GetLocalProcessingConfigRequest.newBuilder()
                .setCustomModsecDetectionRulesHash("Custom")
                .setRegularModsecDetectionRulesHash("Regular")
                .build());

    assertEquals(expectedCustomModsecDetectionRules, response.getCustomModsecDetectionRules());
    assertEquals(expectedRegularModsecDetectionRules, response.getRegularModsecDetectionRules());
  }

  @Test
  @DisplayName("Test get local processing config protection config part")
  void getLocalProcessingConfig_protectionConfig() {
    when(customModsecDetectionManager.getEnabledRules(any()))
        .thenReturn(CustomModsecDetectionRules.getDefaultInstance());
    when(regularModsecDetectionManager.getDetectionRules(any()))
        .thenReturn(RegularModsecDetectionRules.getDefaultInstance());

    setLicenseStatus(LICENSE_LIMIT_AVAILABLE);
    createLocalProcessingRule("/checkout/*", "abc.com", ProtectionMode.PROTECTION_MODE_CORE);
    createLocalProcessingRule("/orders/**", "xyz.com", ProtectionMode.PROTECTION_MODE_ADVANCED);
    ProtectionModeConfig expectedConfig =
        ProtectionModeConfig.newBuilder()
            .setDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_ADVANCED)
            .addProtectedEndpoints(
                ProtectedEndpoint.newBuilder()
                    .setUrlPattern("/orders/**")
                    .setHostHeader("xyz.com")
                    .setProtectionMode(ProtectionMode.PROTECTION_MODE_ADVANCED)
                    .build())
            .addProtectedEndpoints(
                ProtectedEndpoint.newBuilder()
                    .setUrlPattern("/checkout/*")
                    .setHostHeader("abc.com")
                    .setProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
                    .build())
            .build();
    ProtectionModeConfig actualConfig =
        localProcessingConfigStub
            .getLocalProcessingConfig(GetLocalProcessingConfigRequest.getDefaultInstance())
            .getProtectionModeConfig();
    assertEquals(expectedConfig, actualConfig);
  }

  @Test
  void getLocalProcessingConfigWithLimitExhausted_protectionConfig() {
    when(customModsecDetectionManager.getEnabledRules(any()))
        .thenReturn(CustomModsecDetectionRules.getDefaultInstance());
    when(regularModsecDetectionManager.getDetectionRules(any()))
        .thenReturn(RegularModsecDetectionRules.getDefaultInstance());

    setLicenseStatus(LICENSE_LIMIT_EXHAUSTED);
    createLocalProcessingRule("/checkout/*", "abc.com", ProtectionMode.PROTECTION_MODE_CORE);
    createLocalProcessingRule("/orders/**", "xyz.com", ProtectionMode.PROTECTION_MODE_ADVANCED);
    ProtectionModeConfig expectedConfig =
        ProtectionModeConfig.newBuilder()
            .setDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
            .build();
    ProtectionModeConfig actualConfig =
        localProcessingConfigStub
            .getLocalProcessingConfig(GetLocalProcessingConfigRequest.getDefaultInstance())
            .getProtectionModeConfig();
    assertEquals(expectedConfig, actualConfig);
  }

  private LocalProcessingRuleDetails createLocalProcessingRule(
      String urlPattern, String hostHeader, ProtectionMode protectionMode) {
    NewLocalProcessingRule newLocalProcessingRule =
        NewLocalProcessingRule.newBuilder()
            .setUrlPattern(urlPattern)
            .setHostHeader(hostHeader)
            .setProtectionMode(protectionMode)
            .build();
    return localProcessingRulesStub
        .createLocalProcessingRule(
            CreateLocalProcessingRuleRequest.newBuilder()
                .setNewLocalProcessingRule(newLocalProcessingRule)
                .build())
        .getLocalProcessingRuleDetails();
  }

  private void setLicenseStatus(LicenseLimit licenseLimit) {
    licenseStatus = LicenseStatus.newBuilder().setTracesLicenseLimit(licenseLimit).build();
  }

  class MockLicenseStatusConfigService
      extends LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceImplBase {

    @Override
    public void getLicenseStatus(
        GetLicenseStatusRequest request,
        StreamObserver<GetLicenseStatusResponse> responseObserver) {
      responseObserver.onNext(
          GetLicenseStatusResponse.newBuilder().setLicenseStatus(licenseStatus).build());
      responseObserver.onCompleted();
    }
  }
}
