package ai.traceable.localprocessing.config.service.spanprocessingrules;

import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildDefaultExpectedRateLimitConfig;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildDefaultRateLimitConfig_GetAllResolvedSamplingConfigsResponse;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildExpectedExcludeSpanProcessingRule;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildExpectedExcludeSpanProcessingRuleServiceNamesAndEnvironmentsProcessed;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildExpectedProtectionSpanProcessingRule;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildExpectedProtectionSpanProcessingRuleServiceNamesAndEnvironmentsProcessed;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildExpectedRateLimitConfig;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildGetAllExcludeSpanRulesResponse;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildGetAllExcludeSpanRulesResponseEnvironmentFilter;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildGetAllExcludeSpanRulesResponseServiceNameFilter;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildGetAllResolvedProtectionSpanRulesResponse;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildGetAllResolvedProtectionSpanRulesResponseEnvironmentFilter;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildGetAllResolvedProtectionSpanRulesResponseServiceNameFilter;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildGetAllResolvedSamplingConfigsResponse;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildGetAllResolvedSamplingConfigsResponseEnvironmentFilter;
import static ai.traceable.localprocessing.config.service.spanprocessingrules.SpanProcessingRulesManagerTestUtils.buildGetAllResolvedSamplingConfigsResponseServiceNameFilter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules.DefaultExcludeSpanRulesManager;
import ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules.ExcludeSpanRulesManager;
import ai.traceable.localprocessing.config.service.spanprocessingrules.protectionspanrules.DefaultProtectionSpanRulesManager;
import ai.traceable.localprocessing.config.service.spanprocessingrules.protectionspanrules.ProtectionSpanRulesManager;
import ai.traceable.localprocessing.config.service.spanprocessingrules.ratelimitconfig.DefaultRateLimitConfigManager;
import ai.traceable.localprocessing.config.service.spanprocessingrules.ratelimitconfig.RateLimitConfigManager;
import ai.traceable.localprocessing.config.service.utils.FilterConverter;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRule;
import ai.traceable.localprocessing.config.service.v1.GetSpanProcessingRulesRequest;
import ai.traceable.localprocessing.config.service.v1.GetSpanProcessingRulesResponse;
import ai.traceable.localprocessing.config.service.v1.ProtectionSpanProcessingRule;
import ai.traceable.localprocessing.config.service.v1.RateLimitConfig;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRules;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRulesServiceRequest;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRulesServiceResponse;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedProtectionSpanRulesResponse;
import java.util.List;
import org.hypertrace.config.span.processing.utils.SpanFilterMatcher;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesResponse;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultSpanProcessingRulesManagerTest {

  private SpanProcessingRulesManager spanProcessingRulesManager;
  private UuidGenerator uuidGenerator;
  private SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      spanProcessingConfigServiceBlockingStub;
  private ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc
          .SpanProcessingConfigServiceBlockingStub
      traceableSpanProcessingConfigServiceBlockingStub;
  private RequestContext requestContext;

  @BeforeEach
  void setup() {
    spanProcessingConfigServiceBlockingStub =
        mock(SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub.class);
    traceableSpanProcessingConfigServiceBlockingStub =
        mock(
            ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc
                .SpanProcessingConfigServiceBlockingStub.class);
    uuidGenerator = new UuidGenerator();
    ai.traceable.config.utils.SpanFilterMatcher spanFilterMatcher =
        new ai.traceable.config.utils.SpanFilterMatcher();
    FilterConverter filterConverter = new FilterConverter();
    ExcludeSpanRulesManager excludeSpanRulesManager =
        new DefaultExcludeSpanRulesManager(
            spanProcessingConfigServiceBlockingStub, new SpanFilterMatcher());
    ProtectionSpanRulesManager protectionSpanRulesManager =
        new DefaultProtectionSpanRulesManager(
            traceableSpanProcessingConfigServiceBlockingStub, spanFilterMatcher, filterConverter);
    RateLimitConfigManager rateLimitConfigManager =
        new DefaultRateLimitConfigManager(
            traceableSpanProcessingConfigServiceBlockingStub, spanFilterMatcher, filterConverter);
    spanProcessingRulesManager =
        new DefaultSpanProcessingRulesManager(
            excludeSpanRulesManager,
            rateLimitConfigManager,
            protectionSpanRulesManager,
            uuidGenerator);
    requestContext = RequestContext.forTenantId("tenantId");
  }

  @Test
  void testGetAllExcludeSpanRules() {
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedSamplingConfigs(any()))
        .thenReturn(buildDefaultRateLimitConfig_GetAllResolvedSamplingConfigsResponse());
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedProtectionSpanRules(any()))
        .thenReturn(GetAllResolvedProtectionSpanRulesResponse.newBuilder().build());
    when(spanProcessingConfigServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(buildGetAllExcludeSpanRulesResponse(false));

    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .build())
                .build());
    ExcludeSpanProcessingRule expectedExcludeSpanProcessingRule =
        buildExpectedExcludeSpanProcessingRule();
    RateLimitConfig expectedRateLimitConfig = buildExpectedRateLimitConfig();

    String hash =
        uuidGenerator.generateId(
            SpanProcessingRules.newBuilder()
                .setRateLimitConfig(expectedRateLimitConfig)
                .addAllExcludeSpanRules(List.of(expectedExcludeSpanProcessingRule))
                .build());
    SpanProcessingRules expectedSpanProcessingRules =
        SpanProcessingRules.newBuilder()
            .setRateLimitConfig(expectedRateLimitConfig)
            .addExcludeSpanRules(expectedExcludeSpanProcessingRule)
            .build();

    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setSpanProcessingRules(expectedSpanProcessingRules)
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRules))
                    .setServiceName("service1")
                    .build())
            .build(),
        getSpanProcessingRulesResponse);

    getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .setHash(hash)
                        .build())
                .build());
    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setHash(hash)
                    .setServiceName("service1")
                    .build())
            .build(),
        getSpanProcessingRulesResponse);
  }

  @Test
  void testGetAllExcludeSpanRulesEnvironmentFilter() {
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedSamplingConfigs(any()))
        .thenReturn(buildDefaultRateLimitConfig_GetAllResolvedSamplingConfigsResponse());
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedProtectionSpanRules(any()))
        .thenReturn(GetAllResolvedProtectionSpanRulesResponse.newBuilder().build());
    when(spanProcessingConfigServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(buildGetAllExcludeSpanRulesResponseEnvironmentFilter());
    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .setEnvironment("env")
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .setHash("")
                        .build())
                .build());
    List<SpanProcessingRulesServiceResponse> spanProcessingRulesServiceResponses =
        getSpanProcessingRulesResponse.getSpanProcessingRulesServiceResponsesList();

    SpanProcessingRules spanProcessingRules =
        spanProcessingRulesServiceResponses.get(0).getSpanProcessingRules();
    List<ExcludeSpanProcessingRule> excludeSpanProcessingRules =
        spanProcessingRules.getExcludeSpanRulesList();
    RateLimitConfig expectedRateLimitConfig = buildExpectedRateLimitConfig();

    SpanProcessingRules expectedSpanProcessingRules =
        SpanProcessingRules.newBuilder().setRateLimitConfig(expectedRateLimitConfig).build();
    assertEquals(0, excludeSpanProcessingRules.size());
    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setServiceName("service1")
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRules))
                    .setSpanProcessingRules(spanProcessingRules)
                    .build())
            .build(),
        getSpanProcessingRulesResponse);

    getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .setEnvironment("value")
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .setHash("")
                        .build())
                .build());
    spanProcessingRulesServiceResponses =
        getSpanProcessingRulesResponse.getSpanProcessingRulesServiceResponsesList();
    spanProcessingRules = spanProcessingRulesServiceResponses.get(0).getSpanProcessingRules();
    excludeSpanProcessingRules = spanProcessingRules.getExcludeSpanRulesList();

    assertEquals(1, excludeSpanProcessingRules.size());
    ExcludeSpanProcessingRule expectedExcludeSpanProcessingRule =
        buildExpectedExcludeSpanProcessingRuleServiceNamesAndEnvironmentsProcessed();
    expectedSpanProcessingRules =
        SpanProcessingRules.newBuilder()
            .setRateLimitConfig(expectedRateLimitConfig)
            .addExcludeSpanRules(expectedExcludeSpanProcessingRule)
            .build();

    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setServiceName("service1")
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRules))
                    .setSpanProcessingRules(expectedSpanProcessingRules)
                    .build())
            .build(),
        getSpanProcessingRulesResponse);
  }

  @Test
  void testGetAllExcludeSpanRulesServiceNameFilter() {
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedSamplingConfigs(any()))
        .thenReturn(buildDefaultRateLimitConfig_GetAllResolvedSamplingConfigsResponse());
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedProtectionSpanRules(any()))
        .thenReturn(GetAllResolvedProtectionSpanRulesResponse.newBuilder().build());
    when(spanProcessingConfigServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(buildGetAllExcludeSpanRulesResponseServiceNameFilter());

    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("value")
                        .setHash("")
                        .build())
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("vil")
                        .setHash("")
                        .build())
                .build());

    List<SpanProcessingRulesServiceResponse> spanProcessingRulesServiceResponses =
        getSpanProcessingRulesResponse.getSpanProcessingRulesServiceResponsesList();

    SpanProcessingRules spanProcessingRulesFirst =
        spanProcessingRulesServiceResponses.get(0).getSpanProcessingRules();
    List<ExcludeSpanProcessingRule> excludeSpanProcessingRulesFirst =
        spanProcessingRulesFirst.getExcludeSpanRulesList();

    SpanProcessingRules spanProcessingRulesSecond =
        spanProcessingRulesServiceResponses.get(1).getSpanProcessingRules();
    List<ExcludeSpanProcessingRule> excludeSpanProcessingRulesSecond =
        spanProcessingRulesSecond.getExcludeSpanRulesList();

    RateLimitConfig expectedRateLimitConfig = buildExpectedRateLimitConfig();

    SpanProcessingRules expectedSpanProcessingRulesFirst =
        SpanProcessingRules.newBuilder()
            .setRateLimitConfig(expectedRateLimitConfig)
            .addAllExcludeSpanRules(excludeSpanProcessingRulesFirst)
            .build();
    SpanProcessingRules expectedSpanProcessingRulesSecond =
        SpanProcessingRules.newBuilder()
            .setRateLimitConfig(expectedRateLimitConfig)
            .addAllExcludeSpanRules(excludeSpanProcessingRulesSecond)
            .build();

    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setServiceName("value")
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRulesFirst))
                    .setSpanProcessingRules(expectedSpanProcessingRulesFirst)
                    .build())
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setServiceName("vil")
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRulesSecond))
                    .setSpanProcessingRules(expectedSpanProcessingRulesSecond)
                    .build())
            .build(),
        getSpanProcessingRulesResponse);
  }

  @Test
  void testGetAllExcludeSpanRulesRuleDisabled() {
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedSamplingConfigs(any()))
        .thenReturn(buildDefaultRateLimitConfig_GetAllResolvedSamplingConfigsResponse());
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedProtectionSpanRules(any()))
        .thenReturn(GetAllResolvedProtectionSpanRulesResponse.newBuilder().build());
    when(spanProcessingConfigServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(buildGetAllExcludeSpanRulesResponse(true));
    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .build())
                .build());
    List<SpanProcessingRulesServiceResponse> spanProcessingRulesServiceResponses =
        getSpanProcessingRulesResponse.getSpanProcessingRulesServiceResponsesList();

    SpanProcessingRules spanProcessingRules =
        spanProcessingRulesServiceResponses.get(0).getSpanProcessingRules();
    List<ExcludeSpanProcessingRule> excludeSpanProcessingRules =
        spanProcessingRules.getExcludeSpanRulesList();

    SpanProcessingRules expectedSpanProcessingRules =
        SpanProcessingRules.newBuilder().setRateLimitConfig(buildExpectedRateLimitConfig()).build();
    assertEquals(0, excludeSpanProcessingRules.size());
    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setServiceName("service1")
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRules))
                    .setSpanProcessingRules(spanProcessingRules)
                    .build())
            .build(),
        getSpanProcessingRulesResponse);
  }

  @Test
  void testGetAllProtectionSpanRules() {
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedSamplingConfigs(any()))
        .thenReturn(buildDefaultRateLimitConfig_GetAllResolvedSamplingConfigsResponse());
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedProtectionSpanRules(any()))
        .thenReturn(buildGetAllResolvedProtectionSpanRulesResponse(false));
    when(spanProcessingConfigServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(GetAllExcludeSpanRulesResponse.newBuilder().build());

    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .build())
                .build());
    ProtectionSpanProcessingRule expectedProtectionSpanProcessingRule =
        buildExpectedProtectionSpanProcessingRule();
    RateLimitConfig expectedRateLimitConfig = buildExpectedRateLimitConfig();

    String hash =
        uuidGenerator.generateId(
            SpanProcessingRules.newBuilder()
                .setRateLimitConfig(expectedRateLimitConfig)
                .addAllProtectionSpanRules(List.of(expectedProtectionSpanProcessingRule))
                .build());
    SpanProcessingRules expectedSpanProcessingRules =
        SpanProcessingRules.newBuilder()
            .setRateLimitConfig(expectedRateLimitConfig)
            .addProtectionSpanRules(expectedProtectionSpanProcessingRule)
            .build();

    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setSpanProcessingRules(expectedSpanProcessingRules)
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRules))
                    .setServiceName("service1")
                    .build())
            .build(),
        getSpanProcessingRulesResponse);

    getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .setHash(hash)
                        .build())
                .build());
    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setHash(hash)
                    .setServiceName("service1")
                    .build())
            .build(),
        getSpanProcessingRulesResponse);
  }

  @Test
  void testGetAllProtectionSpanRulesEnvironmentFilter() {
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedSamplingConfigs(any()))
        .thenReturn(buildDefaultRateLimitConfig_GetAllResolvedSamplingConfigsResponse());
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedProtectionSpanRules(any()))
        .thenReturn(buildGetAllResolvedProtectionSpanRulesResponseEnvironmentFilter());
    when(spanProcessingConfigServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(GetAllExcludeSpanRulesResponse.newBuilder().build());
    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .setEnvironment("env")
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .setHash("")
                        .build())
                .build());
    List<SpanProcessingRulesServiceResponse> spanProcessingRulesServiceResponses =
        getSpanProcessingRulesResponse.getSpanProcessingRulesServiceResponsesList();
    RateLimitConfig expectedRateLimitConfig = buildExpectedRateLimitConfig();

    SpanProcessingRules spanProcessingRules =
        spanProcessingRulesServiceResponses.get(0).getSpanProcessingRules();
    List<ProtectionSpanProcessingRule> protectionSpanProcessingRules =
        spanProcessingRules.getProtectionSpanRulesList();

    SpanProcessingRules expectedSpanProcessingRules =
        SpanProcessingRules.newBuilder().setRateLimitConfig(expectedRateLimitConfig).build();
    assertEquals(0, protectionSpanProcessingRules.size());
    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setServiceName("service1")
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRules))
                    .setSpanProcessingRules(spanProcessingRules)
                    .build())
            .build(),
        getSpanProcessingRulesResponse);

    getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .setEnvironment("value")
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .setHash("")
                        .build())
                .build());
    spanProcessingRulesServiceResponses =
        getSpanProcessingRulesResponse.getSpanProcessingRulesServiceResponsesList();
    spanProcessingRules = spanProcessingRulesServiceResponses.get(0).getSpanProcessingRules();
    protectionSpanProcessingRules = spanProcessingRules.getProtectionSpanRulesList();

    assertEquals(1, protectionSpanProcessingRules.size());
    ProtectionSpanProcessingRule expectedProtectionSpanProcessingRule =
        buildExpectedProtectionSpanProcessingRuleServiceNamesAndEnvironmentsProcessed();
    expectedSpanProcessingRules =
        SpanProcessingRules.newBuilder()
            .setRateLimitConfig(expectedRateLimitConfig)
            .addProtectionSpanRules(expectedProtectionSpanProcessingRule)
            .build();

    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setServiceName("service1")
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRules))
                    .setSpanProcessingRules(expectedSpanProcessingRules)
                    .build())
            .build(),
        getSpanProcessingRulesResponse);
  }

  @Test
  void testGetAllProtectionSpanRulesServiceNameFilter() {
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedSamplingConfigs(any()))
        .thenReturn(buildDefaultRateLimitConfig_GetAllResolvedSamplingConfigsResponse());
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedProtectionSpanRules(any()))
        .thenReturn(buildGetAllResolvedProtectionSpanRulesResponseServiceNameFilter());
    when(spanProcessingConfigServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(GetAllExcludeSpanRulesResponse.newBuilder().build());

    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("value")
                        .setHash("")
                        .build())
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("vil")
                        .setHash("")
                        .build())
                .build());

    List<SpanProcessingRulesServiceResponse> spanProcessingRulesServiceResponses =
        getSpanProcessingRulesResponse.getSpanProcessingRulesServiceResponsesList();

    SpanProcessingRules spanProcessingRulesFirst =
        spanProcessingRulesServiceResponses.get(0).getSpanProcessingRules();
    List<ProtectionSpanProcessingRule> protectionSpanProcessingRulesFirst =
        spanProcessingRulesFirst.getProtectionSpanRulesList();

    SpanProcessingRules spanProcessingRulesSecond =
        spanProcessingRulesServiceResponses.get(1).getSpanProcessingRules();
    List<ProtectionSpanProcessingRule> protectionSpanProcessingRulesSecond =
        spanProcessingRulesSecond.getProtectionSpanRulesList();

    RateLimitConfig expectedRateLimitConfig = buildExpectedRateLimitConfig();

    SpanProcessingRules expectedSpanProcessingRulesFirst =
        SpanProcessingRules.newBuilder()
            .setRateLimitConfig(expectedRateLimitConfig)
            .addAllProtectionSpanRules(protectionSpanProcessingRulesFirst)
            .build();
    SpanProcessingRules expectedSpanProcessingRulesSecond =
        SpanProcessingRules.newBuilder()
            .setRateLimitConfig(expectedRateLimitConfig)
            .addAllProtectionSpanRules(protectionSpanProcessingRulesSecond)
            .build();

    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setServiceName("value")
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRulesFirst))
                    .setSpanProcessingRules(expectedSpanProcessingRulesFirst)
                    .build())
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setServiceName("vil")
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRulesSecond))
                    .setSpanProcessingRules(expectedSpanProcessingRulesSecond)
                    .build())
            .build(),
        getSpanProcessingRulesResponse);
  }

  @Test
  void testGetAllProtectionSpanRulesRuleDisabled() {
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedSamplingConfigs(any()))
        .thenReturn(buildDefaultRateLimitConfig_GetAllResolvedSamplingConfigsResponse());
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedProtectionSpanRules(any()))
        .thenReturn(buildGetAllResolvedProtectionSpanRulesResponse(true));
    when(spanProcessingConfigServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(GetAllExcludeSpanRulesResponse.newBuilder().build());
    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .build())
                .build());
    List<SpanProcessingRulesServiceResponse> spanProcessingRulesServiceResponses =
        getSpanProcessingRulesResponse.getSpanProcessingRulesServiceResponsesList();

    SpanProcessingRules spanProcessingRules =
        spanProcessingRulesServiceResponses.get(0).getSpanProcessingRules();
    List<ProtectionSpanProcessingRule> protectionSpanProcessingRules =
        spanProcessingRules.getProtectionSpanRulesList();

    SpanProcessingRules expectedSpanProcessingRules =
        SpanProcessingRules.newBuilder().setRateLimitConfig(buildExpectedRateLimitConfig()).build();
    assertEquals(0, protectionSpanProcessingRules.size());
    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setServiceName("service1")
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRules))
                    .setSpanProcessingRules(spanProcessingRules)
                    .build())
            .build(),
        getSpanProcessingRulesResponse);
  }

  @Test
  void testGetRateLimitConfig() {
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedSamplingConfigs(any()))
        .thenReturn(buildGetAllResolvedSamplingConfigsResponse());

    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedProtectionSpanRules(any()))
        .thenReturn(GetAllResolvedProtectionSpanRulesResponse.newBuilder().build());
    when(spanProcessingConfigServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(GetAllExcludeSpanRulesResponse.newBuilder().build());

    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .build())
                .build());
    RateLimitConfig expectedRateLimitConfig = buildExpectedRateLimitConfig();

    String hash =
        uuidGenerator.generateId(
            SpanProcessingRules.newBuilder()
                .setRateLimitConfig(buildExpectedRateLimitConfig())
                .build());
    SpanProcessingRules expectedSpanProcessingRules =
        SpanProcessingRules.newBuilder().setRateLimitConfig(expectedRateLimitConfig).build();

    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setSpanProcessingRules(expectedSpanProcessingRules)
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRules))
                    .setServiceName("service1")
                    .build())
            .build(),
        getSpanProcessingRulesResponse);

    getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .setHash(hash)
                        .build())
                .build());
    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setHash(hash)
                    .setServiceName("service1")
                    .build())
            .build(),
        getSpanProcessingRulesResponse);
  }

  @Test
  void testGetAllSamplingConfigsEnvironmentFilter() {
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedSamplingConfigs(any()))
        .thenReturn(buildGetAllResolvedSamplingConfigsResponseEnvironmentFilter());
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedProtectionSpanRules(any()))
        .thenReturn(GetAllResolvedProtectionSpanRulesResponse.newBuilder().build());
    when(spanProcessingConfigServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(GetAllExcludeSpanRulesResponse.newBuilder().build());
    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .setEnvironment("env")
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .setHash("")
                        .build())
                .build());
    List<SpanProcessingRulesServiceResponse> spanProcessingRulesServiceResponses =
        getSpanProcessingRulesResponse.getSpanProcessingRulesServiceResponsesList();

    SpanProcessingRules spanProcessingRules =
        spanProcessingRulesServiceResponses.get(0).getSpanProcessingRules();

    SpanProcessingRules expectedSpanProcessingRules =
        SpanProcessingRules.newBuilder()
            .setRateLimitConfig(buildDefaultExpectedRateLimitConfig())
            .build();
    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setServiceName("service1")
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRules))
                    .setSpanProcessingRules(spanProcessingRules)
                    .build())
            .build(),
        getSpanProcessingRulesResponse);

    getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .setEnvironment("value")
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .setHash("")
                        .build())
                .build());

    expectedSpanProcessingRules =
        SpanProcessingRules.newBuilder().setRateLimitConfig(buildExpectedRateLimitConfig()).build();

    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setServiceName("service1")
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRules))
                    .setSpanProcessingRules(expectedSpanProcessingRules)
                    .build())
            .build(),
        getSpanProcessingRulesResponse);
  }

  @Test
  void testGetAllSamplingConfigsServiceNameFilter() {
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedSamplingConfigs(any()))
        .thenReturn(buildGetAllResolvedSamplingConfigsResponseServiceNameFilter());
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedProtectionSpanRules(any()))
        .thenReturn(GetAllResolvedProtectionSpanRulesResponse.newBuilder().build());
    when(spanProcessingConfigServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(GetAllExcludeSpanRulesResponse.newBuilder().build());

    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("value")
                        .setHash("")
                        .build())
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("vil")
                        .setHash("")
                        .build())
                .build());

    SpanProcessingRules expectedSpanProcessingRulesFirst =
        SpanProcessingRules.newBuilder().setRateLimitConfig(buildExpectedRateLimitConfig()).build();
    SpanProcessingRules expectedSpanProcessingRulesSecond =
        SpanProcessingRules.newBuilder()
            .setRateLimitConfig(buildDefaultExpectedRateLimitConfig())
            .build();

    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setServiceName("value")
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRulesFirst))
                    .setSpanProcessingRules(expectedSpanProcessingRulesFirst)
                    .build())
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setServiceName("vil")
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRulesSecond))
                    .setSpanProcessingRules(expectedSpanProcessingRulesSecond)
                    .build())
            .build(),
        getSpanProcessingRulesResponse);
  }

  @Test
  void testSpanProcessingRules() {
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedSamplingConfigs(any()))
        .thenReturn(buildGetAllResolvedSamplingConfigsResponse());
    when(traceableSpanProcessingConfigServiceBlockingStub.getAllResolvedProtectionSpanRules(any()))
        .thenReturn(buildGetAllResolvedProtectionSpanRulesResponse(false));
    when(spanProcessingConfigServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(buildGetAllExcludeSpanRulesResponse(false));

    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .build())
                .build());

    ExcludeSpanProcessingRule expectedExcludeSpanProcessingRule =
        buildExpectedExcludeSpanProcessingRule();
    ProtectionSpanProcessingRule expectedProtectionSpanProcessingRule =
        buildExpectedProtectionSpanProcessingRule();
    RateLimitConfig expectedRateLimitConfig = buildExpectedRateLimitConfig();

    String hash =
        uuidGenerator.generateId(
            SpanProcessingRules.newBuilder()
                .setRateLimitConfig(expectedRateLimitConfig)
                .addAllExcludeSpanRules(List.of(expectedExcludeSpanProcessingRule))
                .addAllProtectionSpanRules(List.of(expectedProtectionSpanProcessingRule))
                .build());
    SpanProcessingRules expectedSpanProcessingRules =
        SpanProcessingRules.newBuilder()
            .setRateLimitConfig(expectedRateLimitConfig)
            .addExcludeSpanRules(expectedExcludeSpanProcessingRule)
            .addProtectionSpanRules(expectedProtectionSpanProcessingRule)
            .build();

    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setSpanProcessingRules(expectedSpanProcessingRules)
                    .setHash(uuidGenerator.generateId(expectedSpanProcessingRules))
                    .setServiceName("service1")
                    .build())
            .build(),
        getSpanProcessingRulesResponse);

    getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            requestContext,
            GetSpanProcessingRulesRequest.newBuilder()
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .setHash(hash)
                        .build())
                .build());
    assertEquals(
        GetSpanProcessingRulesResponse.newBuilder()
            .addSpanProcessingRulesServiceResponses(
                SpanProcessingRulesServiceResponse.newBuilder()
                    .setHash(hash)
                    .setServiceName("service1")
                    .build())
            .build(),
        getSpanProcessingRulesResponse);
  }
}
