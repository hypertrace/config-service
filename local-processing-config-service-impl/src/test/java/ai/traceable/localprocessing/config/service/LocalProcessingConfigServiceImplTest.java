package ai.traceable.localprocessing.config.service;

import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_AVAILABLE;
import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_EXHAUSTED;
import static ai.traceable.localprocessing.config.service.ConfigServiceCoordinatorImpl.DEFAULT_PROTECTION_MODE;
import static ai.traceable.localprocessing.config.service.ConfigServiceCoordinatorImpl.LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusRequest;
import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusResponse;
import ai.traceable.licensestatus.config.service.v1.LicenseLimit;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceBlockingStub;
import ai.traceable.localprocessing.config.service.v1.CreateLocalProcessingRuleRequest;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigRequest;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc.LocalProcessingConfigServiceBlockingStub;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleDetails;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRulesServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRulesServiceGrpc.LocalProcessingRulesServiceBlockingStub;
import ai.traceable.localprocessing.config.service.v1.NewLocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ProtectedEndpoint;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import ai.traceable.localprocessing.config.service.v1.ProtectionModeConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LocalProcessingConfigServiceImplTest {

  LocalProcessingConfigServiceBlockingStub localProcessingConfigStub;
  LocalProcessingRulesServiceBlockingStub localProcessingRulesStub;
  LicenseStatusConfigServiceBlockingStub licenseStatusConfigStub;
  MockGenericConfigService mockGenericConfigService;
  LicenseStatus licenseStatus;

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
    Channel channel = mockGenericConfigService.channel();
    mockGenericConfigService
        .addService(new LocalProcessingConfigServiceImpl(channel, config))
        .addService(new LocalProcessingRulesServiceImpl(channel, config))
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
  void getLocalProcessingConfig() {
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
  void getLocalProcessingConfigWithLimitExhausted() {
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
