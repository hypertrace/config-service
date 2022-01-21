package ai.traceable.localprocessing.config.service;

import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_AVAILABLE;
import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_EXHAUSTED;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.DEFAULT_PROTECTION_MODE;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.MODSEC_REDACT_MESSAGES;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.SAMPLING_POLICIES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusRequest;
import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusResponse;
import ai.traceable.licensestatus.config.service.v1.LicenseLimit;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceBlockingStub;
import ai.traceable.localprocessing.config.service.apinaming.ApiNamingManager;
import ai.traceable.localprocessing.config.service.client.EntityDataServiceClient;
import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinator;
import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinatorImpl;
import ai.traceable.localprocessing.config.service.coordinator.DefaultProtectionModeConfigStore;
import ai.traceable.localprocessing.config.service.coordinator.LocalProcessingRulesConfigStore;
import ai.traceable.localprocessing.config.service.customsignature.CustomModsecDetectionManager;
import ai.traceable.localprocessing.config.service.regularmodsec.RegularModsecDetectionManager;
import ai.traceable.localprocessing.config.service.ruleservice.LocalProcessingRulesServiceImpl;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.CreateLocalProcessingRuleRequest;
import ai.traceable.localprocessing.config.service.v1.CustomModsecDetectionRules;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelResponse;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigRequest;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigResponse;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingModelResponse;
import ai.traceable.localprocessing.config.service.v1.HttpServiceResponse;
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
import ai.traceable.localprocessing.config.service.v1.SamplingPolicies;
import ai.traceable.localprocessing.config.service.v1.SamplingPolicy;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
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
  ApiNamingManager apiNamingManager;
  UuidGenerator uuidGenerator;
  EntityDataServiceClient entityDataServiceClient;
  LocalProcessingConfigRequestValidator localProcessingConfigRequestValidator;

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGet().mockGetAll();

    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
    entityDataServiceClient = mock(EntityDataServiceClient.class);
    localProcessingConfigRequestValidator = mock(LocalProcessingConfigRequestValidator.class);

    Map<String, Map> configMap = new HashMap<>();
    configMap.put(
        LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG,
        Map.of(
            DEFAULT_PROTECTION_MODE,
            ProtectionMode.PROTECTION_MODE_ADVANCED.name(),
            SAMPLING_POLICIES,
            List.of(
                Map.of("name", "modsec-sampling", "modsecAnomaly", Map.of()),
                Map.of(
                    "name",
                    "rate-limit-sampling",
                    "rateLimiting",
                    Map.of(
                        "traceLimitPerEndpointPerMinute", 10, "traceLimitGloballyPerMinute", 100)),
                Map.of(
                    "name",
                    "span-attributes-sampling",
                    "spanAttributes",
                    Map.of(
                        "attributesRequiredForSampling",
                        List.of(
                            Map.of(
                                "key",
                                "attribute.key",
                                "values",
                                List.of(
                                    Map.of("stringValue", "true"), Map.of("boolValue", true))))))),
            MODSEC_REDACT_MESSAGES,
            true));
    configMap.put(
        "license.status.config.service",
        Map.of("default.license.limit", "LICENSE_LIMIT_AVAILABLE"));

    Config config = ConfigFactory.parseMap(configMap);
    ManagedChannel channel = (ManagedChannel) mockGenericConfigService.channel();

    customModsecDetectionManager = mock(CustomModsecDetectionManager.class);
    regularModsecDetectionManager = mock(RegularModsecDetectionManager.class);
    apiNamingManager = mock(ApiNamingManager.class);
    uuidGenerator = new UuidGenerator();

    ConfigServiceCoordinator configServiceCoordinator =
        new ConfigServiceCoordinatorImpl(
            new LocalProcessingConfigServiceConfig(config),
            new DefaultProtectionModeConfigStore(
                configServiceBlockingStub, configChangeEventGenerator),
            new LocalProcessingRulesConfigStore(
                configServiceBlockingStub, configChangeEventGenerator));
    mockGenericConfigService
        .addService(
            new LocalProcessingConfigServiceImpl(
                channel,
                configServiceCoordinator,
                customModsecDetectionManager,
                regularModsecDetectionManager,
                apiNamingManager,
                uuidGenerator,
                localProcessingConfigRequestValidator))
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
  @DisplayName("Test api naming model api naming config part")
  void getApiNamingModel_ApiNamingConfig() {
    List<HttpServiceResponse> serviceResponseList =
        List.of(
            HttpServiceResponse.newBuilder()
                .setServiceName("serviceName")
                .setHttpConfig(HttpApiNamingConfig.newBuilder().setHash("hash").build())
                .build());
    List<String> fallbackRegexList = List.of("fallbackRegex");
    doNothing()
        .when(localProcessingConfigRequestValidator)
        .validateOrThrow(any(RequestContext.class), any(GetApiNamingModelRequest.class));
    when(apiNamingManager.getHttpServiceResponseList(any(), any())).thenReturn(serviceResponseList);
    when(apiNamingManager.getFallbackWildcardRegexes()).thenReturn(fallbackRegexList);

    GetApiNamingModelResponse expectedResponse =
        GetApiNamingModelResponse.newBuilder()
            .setHttpApiNamingResponse(
                HttpApiNamingModelResponse.newBuilder()
                    .addAllHttpServiceResponses(serviceResponseList)
                    .addAllFallbackWildcardRegexes(fallbackRegexList)
                    .build())
            .build();
    assertEquals(
        expectedResponse,
        localProcessingConfigStub.getApiNamingModel(GetApiNamingModelRequest.newBuilder().build()));
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
    assertEquals(true, response.getModsecConfig().getRedactMessages());
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

    List<ProtectedEndpoint> protectedEndpoints =
        List.of(
            ProtectedEndpoint.newBuilder()
                .setUrlPattern("/orders/**")
                .setHostHeader("xyz.com")
                .setProtectionMode(ProtectionMode.PROTECTION_MODE_ADVANCED)
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

    String protectionModeHash = uuidGenerator.generateId(protectionModeConfig);

    ProtectionModeConfig expectedConfig =
        ProtectionModeConfig.newBuilder()
            .setDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_ADVANCED)
            .addAllProtectedEndpoints(protectedEndpoints)
            .setHash(protectionModeHash)
            .build();

    ProtectionModeConfig actualConfig =
        localProcessingConfigStub
            .getLocalProcessingConfig(GetLocalProcessingConfigRequest.getDefaultInstance())
            .getProtectionModeConfig();
    assertEquals(expectedConfig, actualConfig);

    // next call, hash specified in request, empty protection mode config from server
    // expected
    expectedConfig = ProtectionModeConfig.newBuilder().setHash(protectionModeHash).build();
    actualConfig =
        localProcessingConfigStub
            .getLocalProcessingConfig(
                GetLocalProcessingConfigRequest.newBuilder()
                    .setProtectionModeHash(actualConfig.getHash())
                    .build())
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
    expectedConfig =
        expectedConfig.toBuilder().setHash(uuidGenerator.generateId(expectedConfig)).build();
    ProtectionModeConfig actualConfig =
        localProcessingConfigStub
            .getLocalProcessingConfig(GetLocalProcessingConfigRequest.getDefaultInstance())
            .getProtectionModeConfig();
    assertEquals(expectedConfig, actualConfig);
  }

  @Test
  void getSamplingPoliciesConfig() {
    when(customModsecDetectionManager.getEnabledRules(any()))
        .thenReturn(CustomModsecDetectionRules.getDefaultInstance());
    when(regularModsecDetectionManager.getDetectionRules(any()))
        .thenReturn(RegularModsecDetectionRules.getDefaultInstance());
    setLicenseStatus(LICENSE_LIMIT_AVAILABLE);

    SamplingPolicies samplingPolicies =
        localProcessingConfigStub
            .getLocalProcessingConfig(GetLocalProcessingConfigRequest.getDefaultInstance())
            .getSamplingPolicies();
    assertEquals(3, samplingPolicies.getPoliciesCount());

    assertEquals(
        SamplingPolicy.PolicyConfigCase.MODSEC_ANOMALY,
        samplingPolicies.getPolicies(0).getPolicyConfigCase());
    assertEquals("modsec-sampling", samplingPolicies.getPolicies(0).getName());

    assertEquals(
        SamplingPolicy.PolicyConfigCase.RATE_LIMITING,
        samplingPolicies.getPolicies(1).getPolicyConfigCase());
    assertEquals("rate-limit-sampling", samplingPolicies.getPolicies(1).getName());
    assertEquals(
        10, samplingPolicies.getPolicies(1).getRateLimiting().getTraceLimitPerEndpointPerMinute());
    assertEquals(
        100, samplingPolicies.getPolicies(1).getRateLimiting().getTraceLimitGloballyPerMinute());

    assertEquals(
        SamplingPolicy.PolicyConfigCase.SPAN_ATTRIBUTES,
        samplingPolicies.getPolicies(2).getPolicyConfigCase());
    assertEquals("span-attributes-sampling", samplingPolicies.getPolicies(2).getName());
    assertEquals(
        1,
        samplingPolicies
            .getPolicies(2)
            .getSpanAttributes()
            .getAttributesRequiredForSamplingCount());
    assertEquals(
        "attribute.key",
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
