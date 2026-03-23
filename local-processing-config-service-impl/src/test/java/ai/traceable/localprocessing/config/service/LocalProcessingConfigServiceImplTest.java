package ai.traceable.localprocessing.config.service;

import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.DEFAULT_PROTECTION_MODE;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.MODSEC_REDACT_MESSAGES;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.SAMPLING_POLICIES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.apinaming.http.HttpApiNamingManager;
import ai.traceable.localprocessing.config.service.client.EntityQueryServiceClient;
import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinator;
import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinatorImpl;
import ai.traceable.localprocessing.config.service.coordinator.DefaultProtectionModeConfigStore;
import ai.traceable.localprocessing.config.service.coordinator.DetectionRulesConfigStore;
import ai.traceable.localprocessing.config.service.coordinator.LocalProcessingRulesConfigStore;
import ai.traceable.localprocessing.config.service.customsignature.CustomModsecDetectionManager;
import ai.traceable.localprocessing.config.service.customsignature.DefaultCustomModsecDetectionManager;
import ai.traceable.localprocessing.config.service.regularmodsec.DefaultRegularModsecDetectionManager;
import ai.traceable.localprocessing.config.service.regularmodsec.RegularModsecDetectionManager;
import ai.traceable.localprocessing.config.service.ruleservice.LocalProcessingRulesServiceImpl;
import ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManager;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.CreateLocalProcessingRuleRequest;
import ai.traceable.localprocessing.config.service.v1.CustomModsecDetectionRules;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelResponse;
import ai.traceable.localprocessing.config.service.v1.GetDetectionRulesRequest;
import ai.traceable.localprocessing.config.service.v1.GetDetectionRulesResponse;
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
import ai.traceable.localprocessing.config.service.v1.ModsecConfig;
import ai.traceable.localprocessing.config.service.v1.NewLocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ProtectedEndpoint;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import ai.traceable.localprocessing.config.service.v1.ProtectionModeConfig;
import ai.traceable.localprocessing.config.service.v1.RegularModsecDetectionRules;
import ai.traceable.localprocessing.config.service.v1.SamplingPolicies;
import ai.traceable.localprocessing.config.service.v1.SamplingPolicy;
import ai.traceable.localprocessing.config.service.v1.UpdateDetectionRulesEnabledRequest;
import com.google.protobuf.Duration;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.hypertrace.config.objectstore.ClientConfig;
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
  MockGenericConfigService mockGenericConfigService;
  CustomModsecDetectionManager customModsecDetectionManager;
  RegularModsecDetectionManager regularModsecDetectionManager;
  HttpApiNamingManager httpApiNamingManager;
  SpanProcessingRulesManager spanProcessingRulesManager;
  UuidGenerator uuidGenerator;
  EntityQueryServiceClient entityQueryServiceClient;
  LocalProcessingConfigRequestValidator localProcessingConfigRequestValidator;
  FeatureCachingClient featureCachingClient;
  SemanticVersioningComparator semanticVersioningComparator;

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGet().mockGetAll();

    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
    entityQueryServiceClient = mock(EntityQueryServiceClient.class);
    localProcessingConfigRequestValidator = mock(LocalProcessingConfigRequestValidator.class);
    featureCachingClient = mock(FeatureCachingClient.class);
    semanticVersioningComparator = new SemanticVersioningComparator();

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
    Channel channel = mockGenericConfigService.channel();

    httpApiNamingManager = mock(HttpApiNamingManager.class);
    spanProcessingRulesManager = mock(SpanProcessingRulesManager.class);
    uuidGenerator = new UuidGenerator();
    customModsecDetectionManager =
        spy(
            new DefaultCustomModsecDetectionManager(
                mock(
                    CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub
                        .class),
                uuidGenerator,
                ClientConfig.DEFAULT));
    regularModsecDetectionManager =
        spy(
            new DefaultRegularModsecDetectionManager(
                mock(AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub.class),
                uuidGenerator,
                ClientConfig.DEFAULT));

    ConfigServiceCoordinator configServiceCoordinator =
        new ConfigServiceCoordinatorImpl(
            new LocalProcessingConfigServiceConfig(config),
            new DefaultProtectionModeConfigStore(
                configServiceBlockingStub, configChangeEventGenerator),
            new DetectionRulesConfigStore(configServiceBlockingStub, configChangeEventGenerator),
            new LocalProcessingRulesConfigStore(
                configServiceBlockingStub, configChangeEventGenerator));
    mockGenericConfigService
        .addService(
            new LocalProcessingConfigServiceImpl(
                configServiceCoordinator,
                customModsecDetectionManager,
                regularModsecDetectionManager,
                httpApiNamingManager,
                spanProcessingRulesManager,
                uuidGenerator,
                localProcessingConfigRequestValidator,
                featureCachingClient,
                semanticVersioningComparator))
        .addService(new LocalProcessingRulesServiceImpl(configServiceCoordinator))
        .start();

    localProcessingConfigStub = LocalProcessingConfigServiceGrpc.newBlockingStub(channel);
    localProcessingRulesStub = LocalProcessingRulesServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void testGetLocalProcessingConfigWithModSecDisabled() {
    when(featureCachingClient.isTpaModSecProcessingDisabled(any())).thenReturn(true);
    GetLocalProcessingConfigResponse localProcessingConfigResponse =
        localProcessingConfigStub.getLocalProcessingConfig(
            GetLocalProcessingConfigRequest.newBuilder()
                .setCustomModsecDetectionRulesHash("old hash")
                .build());
    assertEquals(
        customModsecDetectionManager.getEmptyRules(),
        localProcessingConfigResponse.getCustomModsecDetectionRules());
    assertEquals(
        regularModsecDetectionManager.getEmptyRules(),
        localProcessingConfigResponse.getRegularModsecDetectionRules());
  }

  @Test
  void testGetLocalProcessingConfigWithModSecEnabled() {
    CustomModsecDetectionRules expectedCustomModsecDetectionRules =
        CustomModsecDetectionRules.newBuilder()
            .setCustomModsecDetectionRulesBlob("some blob")
            .setHash("some hash")
            .build();
    doReturn(expectedCustomModsecDetectionRules)
        .when(customModsecDetectionManager)
        .getEnabledRules(any(), any(), eq(false), any());
    RegularModsecDetectionRules expectedRegularModsecDetectionRules =
        RegularModsecDetectionRules.newBuilder()
            .setRegularModsecDetectionRulesBlob("some blob")
            .setHash("some hash")
            .build();
    doReturn(expectedRegularModsecDetectionRules)
        .when(regularModsecDetectionManager)
        .getDetectionRules(any(), any(), eq(false), eq(false), any());
    GetLocalProcessingConfigResponse localProcessingConfigResponse =
        localProcessingConfigStub.getLocalProcessingConfig(
            GetLocalProcessingConfigRequest.newBuilder()
                .setCustomModsecDetectionRulesHash("old hash")
                .build());
    assertEquals(
        expectedCustomModsecDetectionRules,
        localProcessingConfigResponse.getCustomModsecDetectionRules());
    assertEquals(
        expectedRegularModsecDetectionRules,
        localProcessingConfigResponse.getRegularModsecDetectionRules());
  }

  @Test
  void testGetLocalProcessingConfigWithModSecCorazaEnabled() {
    when(featureCachingClient.isTpaCorazaBasedEvaluationEnabled(any())).thenReturn(true);
    CustomModsecDetectionRules expectedCustomModsecDetectionRules =
        CustomModsecDetectionRules.newBuilder()
            .setCustomModsecDetectionRulesBlob("some blob")
            .setHash("some hash")
            .build();
    doReturn(expectedCustomModsecDetectionRules)
        .when(customModsecDetectionManager)
        .getEnabledRules(any(), any(), eq(true), eq("environmentId"));
    RegularModsecDetectionRules expectedRegularModsecDetectionRules =
        RegularModsecDetectionRules.newBuilder()
            .setRegularModsecDetectionRulesBlob("some blob")
            .setHash("some hash")
            .build();
    doReturn(expectedRegularModsecDetectionRules)
        .when(regularModsecDetectionManager)
        .getDetectionRules(any(), any(), eq(true), eq(false), eq("environmentId"));
    GetLocalProcessingConfigResponse localProcessingConfigResponse =
        localProcessingConfigStub.getLocalProcessingConfig(
            GetLocalProcessingConfigRequest.newBuilder()
                .setCustomModsecDetectionRulesHash("old hash")
                .setAgentCapabilities(
                    GetLocalProcessingConfigRequest.AgentCapabilities.newBuilder()
                        .addComponents(
                            GetLocalProcessingConfigRequest.Component.newBuilder()
                                .setTraceablePlatformAgentVersion("1.50.0")
                                .build())
                        .build())
                .setEnvironment("environmentId")
                .build());
    assertEquals(
        expectedCustomModsecDetectionRules,
        localProcessingConfigResponse.getCustomModsecDetectionRules());
    assertEquals(
        expectedRegularModsecDetectionRules,
        localProcessingConfigResponse.getRegularModsecDetectionRules());
    assertEquals(
        ModsecConfig.RulesBlobType.RULES_BLOB_TYPE_CORAZA,
        localProcessingConfigResponse.getModsecConfig().getRulesBlobType());
  }

  @Test
  @DisplayName("Test api naming model")
  void getApiNamingModel() throws ExecutionException {
    List<HttpServiceResponse> serviceResponseList =
        List.of(
            HttpServiceResponse.newBuilder()
                .setServiceName("serviceName")
                .setHttpConfig(HttpApiNamingConfig.newBuilder().setHash("hash").build())
                .build());
    doNothing()
        .when(localProcessingConfigRequestValidator)
        .validateOrThrow(any(RequestContext.class), any(GetApiNamingModelRequest.class));
    when(httpApiNamingManager.getHttpServiceResponseList(any(), any()))
        .thenReturn(serviceResponseList);
    when(httpApiNamingManager.getAgentPollingFrequency())
        .thenReturn(Duration.newBuilder().setSeconds(3003).build());

    GetApiNamingModelResponse expectedResponse =
        GetApiNamingModelResponse.newBuilder()
            .setHttpApiNamingResponse(
                HttpApiNamingModelResponse.newBuilder()
                    .addAllHttpServiceResponses(serviceResponseList)
                    .build())
            .setRefreshAfterDuration(Duration.newBuilder().setSeconds(3003).build())
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

    doReturn(expectedCustomModsecDetectionRules)
        .when(customModsecDetectionManager)
        .getEnabledRules(any(RequestContext.class), eq("Custom"), anyBoolean(), any());
    doReturn(expectedRegularModsecDetectionRules)
        .when(regularModsecDetectionManager)
        .getDetectionRules(
            any(RequestContext.class), eq("Regular"), anyBoolean(), anyBoolean(), any());

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
    assertTrue(response.getModsecConfig().getRedactMessages());
  }

  @Test
  @DisplayName("Test get local processing config protection config part")
  void getLocalProcessingConfig_protectionConfig() {
    doReturn(CustomModsecDetectionRules.getDefaultInstance())
        .when(customModsecDetectionManager)
        .getEnabledRules(any(), any(), anyBoolean(), any());
    doReturn(RegularModsecDetectionRules.getDefaultInstance())
        .when(regularModsecDetectionManager)
        .getDetectionRules(any(), any(), anyBoolean(), anyBoolean(), any());

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
  void getSamplingPoliciesConfig() {
    doReturn(CustomModsecDetectionRules.getDefaultInstance())
        .when(customModsecDetectionManager)
        .getEnabledRules(any(), any(), anyBoolean(), any());
    doReturn(RegularModsecDetectionRules.getDefaultInstance())
        .when(regularModsecDetectionManager)
        .getDetectionRules(any(), any(), anyBoolean(), anyBoolean(), any());

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

  @Test
  @DisplayName("Test getDetectionRules always returns modsec format rules")
  void testGetDetectionRulesAlwaysReturnsModsecFormat() {
    CustomModsecDetectionRules expectedCustomModsecDetectionRules =
        CustomModsecDetectionRules.newBuilder()
            .setCustomModsecDetectionRulesBlob("modsec custom blob")
            .setHash("custom-hash")
            .build();
    doReturn(expectedCustomModsecDetectionRules)
        .when(customModsecDetectionManager)
        .getEnabledRules(any(), any(), eq(false), any());
    RegularModsecDetectionRules expectedRegularModsecDetectionRules =
        RegularModsecDetectionRules.newBuilder()
            .setRegularModsecDetectionRulesBlob("modsec regular blob")
            .setHash("regular-hash")
            .build();
    doReturn(expectedRegularModsecDetectionRules)
        .when(regularModsecDetectionManager)
        .getDetectionRules(any(), any(), eq(false), eq(false), any());

    GetDetectionRulesResponse response =
        localProcessingConfigStub.getDetectionRules(
            GetDetectionRulesRequest.newBuilder()
                .setCustomModsecDetectionRulesHash("old hash")
                .setEnvironment("environmentId")
                .build());
    assertEquals(expectedCustomModsecDetectionRules, response.getCustomModsecDetectionRules());
    assertEquals(expectedRegularModsecDetectionRules, response.getRegularModsecDetectionRules());
  }

  @Test
  @DisplayName("Test getDetectionRules with environment scoping")
  void testGetDetectionRulesWithEnvironment() {
    CustomModsecDetectionRules expectedCustomRules =
        CustomModsecDetectionRules.newBuilder()
            .setCustomModsecDetectionRulesBlob("env-scoped blob")
            .setHash("env-hash")
            .build();
    doReturn(expectedCustomRules)
        .when(customModsecDetectionManager)
        .getEnabledRules(any(), eq("custom-hash"), eq(false), eq("prod"));
    RegularModsecDetectionRules expectedRegularRules =
        RegularModsecDetectionRules.newBuilder()
            .setRegularModsecDetectionRulesBlob("env-scoped regular blob")
            .setHash("env-regular-hash")
            .build();
    doReturn(expectedRegularRules)
        .when(regularModsecDetectionManager)
        .getDetectionRules(any(), eq("regular-hash"), eq(false), eq(false), eq("prod"));

    GetDetectionRulesResponse response =
        localProcessingConfigStub.getDetectionRules(
            GetDetectionRulesRequest.newBuilder()
                .setCustomModsecDetectionRulesHash("custom-hash")
                .setRegularModsecDetectionRulesHash("regular-hash")
                .setEnvironment("prod")
                .build());
    assertEquals(expectedCustomRules, response.getCustomModsecDetectionRules());
    assertEquals(expectedRegularRules, response.getRegularModsecDetectionRules());
  }

  @Test
  @DisplayName(
      "Test getDetectionRules returns empty rules when detection rules serving is disabled")
  void testGetDetectionRulesWithDetectionRulesDisabled() {
    localProcessingRulesStub.updateDetectionRulesEnabled(
        UpdateDetectionRulesEnabledRequest.newBuilder().setDetectionRulesEnabled(false).build());

    GetDetectionRulesResponse response =
        localProcessingConfigStub.getDetectionRules(GetDetectionRulesRequest.getDefaultInstance());
    assertEquals(
        customModsecDetectionManager.getEmptyRules(), response.getCustomModsecDetectionRules());
    assertEquals(
        regularModsecDetectionManager.getEmptyRules(), response.getRegularModsecDetectionRules());
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
}
