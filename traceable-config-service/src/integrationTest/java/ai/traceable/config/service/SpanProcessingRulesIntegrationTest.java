package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.localprocessing.config.service.v1.GetSpanProcessingRulesRequest;
import ai.traceable.localprocessing.config.service.v1.GetSpanProcessingRulesResponse;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc.LocalProcessingConfigServiceBlockingStub;
import ai.traceable.localprocessing.config.service.v1.PercentageLimitConfig;
import ai.traceable.localprocessing.config.service.v1.SpanLimitingStrategy;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRules;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRulesServiceRequest;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRulesServiceResponse;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.Field;
import ai.traceable.span.processing.config.service.v1.RateLimit;
import ai.traceable.span.processing.config.service.v1.RateLimitConfig;
import ai.traceable.span.processing.config.service.v1.RateLimitStrategy;
import ai.traceable.span.processing.config.service.v1.RelationalOperator;
import ai.traceable.span.processing.config.service.v1.RelationalSpanFilterExpression;
import ai.traceable.span.processing.config.service.v1.SamplingConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfigInfo;
import ai.traceable.span.processing.config.service.v1.SpanFilter;
import ai.traceable.span.processing.config.service.v1.SpanFilterValue;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfig;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.WindowedRateLimit;
import com.google.protobuf.Duration;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SpanProcessingRulesIntegrationTest extends TraceableConfigServiceIntegrationTestBase {

  private static LocalProcessingConfigServiceBlockingStub localProcessingConfigStub;
  private static SpanProcessingConfigServiceBlockingStub spanProcessingConfigStub;

  @BeforeAll
  static void init() {
    localProcessingConfigStub =
        LocalProcessingConfigServiceGrpc.newBlockingStub(channelForExternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    spanProcessingConfigStub =
        SpanProcessingConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testNoConfigs_emptyPercentageLimitConfigs() {
    SpanProcessingRules rules = getSpanProcessingRules("service1");
    assertTrue(rules.getPercentageLimitConfigsList().isEmpty());
  }

  @Test
  void testCreateWithPercentageLimitConfigOnly() {
    String configId =
        createPercentageLimitConfig(
            50,
            ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy
                .SPAN_LIMITING_STRATEGY_DROP);

    PercentageLimitConfig config =
        getSpanProcessingRules("service1").getPercentageLimitConfigsList().get(0);

    assertEquals(configId, config.getId());
    assertEquals(50f, config.getAllowedPercentage());
    assertEquals(SpanLimitingStrategy.SPAN_LIMITING_STRATEGY_DROP, config.getLimitingStrategy());
  }

  @Test
  void testCreateWithBothRateLimitAndPercentageLimitConfig() {
    String configId =
        createSamplingConfig(
            SamplingConfigInfo.newBuilder()
                .setRateLimitConfig(buildRateLimitConfig())
                .setPercentageLimitConfig(
                    ai.traceable.span.processing.config.service.v1.PercentageLimitConfig
                        .newBuilder()
                        .setAllowedPercentage(75)
                        .setLimitingStrategy(
                            ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy
                                .SPAN_LIMITING_STRATEGY_BARESPAN)
                        .build())
                .build());

    SpanProcessingRules rules = getSpanProcessingRules("service1");

    assertEquals(1, rules.getPercentageLimitConfigsList().size());
    PercentageLimitConfig config = rules.getPercentageLimitConfigsList().get(0);
    assertEquals(configId, config.getId());
    assertEquals(75f, config.getAllowedPercentage());
    assertEquals(
        SpanLimitingStrategy.SPAN_LIMITING_STRATEGY_BARESPAN, config.getLimitingStrategy());
  }

  @Test
  void testRateLimitOnlyConfig_noPercentageLimitReturned() {
    createSamplingConfig(
        SamplingConfigInfo.newBuilder().setRateLimitConfig(buildRateLimitConfig()).build());

    SpanProcessingRules rules = getSpanProcessingRules("service1");
    assertTrue(rules.getPercentageLimitConfigsList().isEmpty());
  }

  @Test
  void testUpdatePercentageLimitConfig() {
    String configId =
        createPercentageLimitConfig(
            50,
            ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy
                .SPAN_LIMITING_STRATEGY_DROP);

    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            spanProcessingConfigStub.updateSamplingConfig(
                UpdateSamplingConfigRequest.newBuilder()
                    .setSamplingConfig(
                        UpdateSamplingConfig.newBuilder()
                            .setId(configId)
                            .setPercentageLimitConfig(
                                ai.traceable.span.processing.config.service.v1.PercentageLimitConfig
                                    .newBuilder()
                                    .setAllowedPercentage(80)
                                    .setLimitingStrategy(
                                        ai.traceable.span.processing.config.service.v1
                                            .SpanLimitingStrategy.SPAN_LIMITING_STRATEGY_BARESPAN)
                                    .build())
                            .build())
                    .build()));

    PercentageLimitConfig config =
        getSpanProcessingRules("service1").getPercentageLimitConfigsList().get(0);

    assertEquals(configId, config.getId());
    assertEquals(80f, config.getAllowedPercentage());
    assertEquals(
        SpanLimitingStrategy.SPAN_LIMITING_STRATEGY_BARESPAN, config.getLimitingStrategy());
  }

  @Test
  void testDeleteSamplingConfig_removesPercentageLimitConfig() {
    String configId =
        createPercentageLimitConfig(
            50,
            ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy
                .SPAN_LIMITING_STRATEGY_DROP);

    assertFalse(getSpanProcessingRules("service1").getPercentageLimitConfigsList().isEmpty());

    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            spanProcessingConfigStub.deleteSamplingConfig(
                DeleteSamplingConfigRequest.newBuilder().setId(configId).build()));

    assertTrue(getSpanProcessingRules("service1").getPercentageLimitConfigsList().isEmpty());
  }

  @Test
  void testMultiplePercentageLimitConfigs() {
    String id1 =
        createPercentageLimitConfig(
            30,
            ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy
                .SPAN_LIMITING_STRATEGY_DROP);
    String id2 =
        createPercentageLimitConfig(
            70,
            ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy
                .SPAN_LIMITING_STRATEGY_BARESPAN);

    List<PercentageLimitConfig> configs =
        getSpanProcessingRules("service1").getPercentageLimitConfigsList();
    assertEquals(2, configs.size());

    assertTrue(
        configs.stream().anyMatch(c -> c.getId().equals(id1) && c.getAllowedPercentage() == 30f));
    assertTrue(
        configs.stream().anyMatch(c -> c.getId().equals(id2) && c.getAllowedPercentage() == 70f));
  }

  @Test
  void testEnvironmentFilter_matchingEnvironment() {
    createSamplingConfig(
        SamplingConfigInfo.newBuilder()
            .setPercentageLimitConfig(
                ai.traceable.span.processing.config.service.v1.PercentageLimitConfig.newBuilder()
                    .setAllowedPercentage(40)
                    .setLimitingStrategy(
                        ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy
                            .SPAN_LIMITING_STRATEGY_DROP)
                    .build())
            .setFilter(buildEnvironmentFilter("prod"))
            .build());

    assertTrue(
        getSpanProcessingRulesWithEnv("service1", "staging")
            .getPercentageLimitConfigsList()
            .isEmpty());

    assertEquals(
        1,
        getSpanProcessingRulesWithEnv("service1", "prod").getPercentageLimitConfigsList().size());
    assertEquals(
        40f,
        getSpanProcessingRulesWithEnv("service1", "prod")
            .getPercentageLimitConfigsList()
            .get(0)
            .getAllowedPercentage());
  }

  @Test
  void testServiceNameFilter_matchingServiceName() {
    createSamplingConfig(
        SamplingConfigInfo.newBuilder()
            .setPercentageLimitConfig(
                ai.traceable.span.processing.config.service.v1.PercentageLimitConfig.newBuilder()
                    .setAllowedPercentage(60)
                    .setLimitingStrategy(
                        ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy
                            .SPAN_LIMITING_STRATEGY_BARESPAN)
                    .build())
            .setFilter(buildServiceNameFilter("payment-service"))
            .build());

    assertTrue(getSpanProcessingRules("other-service").getPercentageLimitConfigsList().isEmpty());

    assertEquals(
        1, getSpanProcessingRules("payment-service").getPercentageLimitConfigsList().size());
  }

  @Test
  void testMultipleServiceRequests_independentResults() {
    createSamplingConfig(
        SamplingConfigInfo.newBuilder()
            .setPercentageLimitConfig(
                ai.traceable.span.processing.config.service.v1.PercentageLimitConfig.newBuilder()
                    .setAllowedPercentage(50)
                    .setLimitingStrategy(
                        ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy
                            .SPAN_LIMITING_STRATEGY_DROP)
                    .build())
            .setFilter(buildServiceNameFilter("svc-a"))
            .build());

    GetSpanProcessingRulesResponse response =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                localProcessingConfigStub.getSpanProcessingRules(
                    GetSpanProcessingRulesRequest.newBuilder()
                        .addServiceRequests(
                            SpanProcessingRulesServiceRequest.newBuilder()
                                .setServiceName("svc-a")
                                .build())
                        .addServiceRequests(
                            SpanProcessingRulesServiceRequest.newBuilder()
                                .setServiceName("svc-b")
                                .build())
                        .build()));

    List<SpanProcessingRulesServiceResponse> responses =
        response.getSpanProcessingRulesServiceResponsesList();
    assertEquals(2, responses.size());

    SpanProcessingRulesServiceResponse svcAResponse =
        responses.stream()
            .filter(r -> r.getServiceName().equals("svc-a"))
            .findFirst()
            .orElseThrow();
    SpanProcessingRulesServiceResponse svcBResponse =
        responses.stream()
            .filter(r -> r.getServiceName().equals("svc-b"))
            .findFirst()
            .orElseThrow();

    assertEquals(1, svcAResponse.getSpanProcessingRules().getPercentageLimitConfigsList().size());
    assertTrue(svcBResponse.getSpanProcessingRules().getPercentageLimitConfigsList().isEmpty());
    assertNotEquals(svcAResponse.getHash(), svcBResponse.getHash());
  }

  @Test
  void testCreatePercentageLimitConfig_truncatesExcessDecimals() {
    String configId =
        createPercentageLimitConfig(
            50.12345f,
            ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy
                .SPAN_LIMITING_STRATEGY_DROP);

    PercentageLimitConfig config =
        getSpanProcessingRules("service1").getPercentageLimitConfigsList().get(0);

    assertEquals(configId, config.getId());
    assertEquals(50.12f, config.getAllowedPercentage());
  }

  @Test
  void testUpdatePercentageLimitConfig_truncatesExcessDecimals() {
    String configId =
        createPercentageLimitConfig(
            50,
            ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy
                .SPAN_LIMITING_STRATEGY_DROP);

    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            spanProcessingConfigStub.updateSamplingConfig(
                UpdateSamplingConfigRequest.newBuilder()
                    .setSamplingConfig(
                        UpdateSamplingConfig.newBuilder()
                            .setId(configId)
                            .setPercentageLimitConfig(
                                ai.traceable.span.processing.config.service.v1.PercentageLimitConfig
                                    .newBuilder()
                                    .setAllowedPercentage(99.999f)
                                    .setLimitingStrategy(
                                        ai.traceable.span.processing.config.service.v1
                                            .SpanLimitingStrategy.SPAN_LIMITING_STRATEGY_BARESPAN)
                                    .build())
                            .build())
                    .build()));

    PercentageLimitConfig config =
        getSpanProcessingRules("service1").getPercentageLimitConfigsList().get(0);

    assertEquals(configId, config.getId());
    assertEquals(99.99f, config.getAllowedPercentage());
  }

  @Test
  void testCreatePercentageLimitConfig_preservesTwoDecimalPlaces() {
    String configId =
        createPercentageLimitConfig(
            75.5f,
            ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy
                .SPAN_LIMITING_STRATEGY_DROP);

    PercentageLimitConfig config =
        getSpanProcessingRules("service1").getPercentageLimitConfigsList().get(0);

    assertEquals(configId, config.getId());
    assertEquals(75.5f, config.getAllowedPercentage());
  }

  private String createPercentageLimitConfig(
      float allowedPercentage,
      ai.traceable.span.processing.config.service.v1.SpanLimitingStrategy strategy) {
    return createSamplingConfig(
        SamplingConfigInfo.newBuilder()
            .setPercentageLimitConfig(
                ai.traceable.span.processing.config.service.v1.PercentageLimitConfig.newBuilder()
                    .setAllowedPercentage(allowedPercentage)
                    .setLimitingStrategy(strategy)
                    .build())
            .build());
  }

  private String createSamplingConfig(SamplingConfigInfo samplingConfigInfo) {
    SamplingConfig config =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    spanProcessingConfigStub.createSamplingConfig(
                        CreateSamplingConfigRequest.newBuilder()
                            .setSamplingConfigInfo(samplingConfigInfo)
                            .build()))
            .getSamplingConfigDetails()
            .getSamplingConfig();
    return config.getId();
  }

  private SpanProcessingRules getSpanProcessingRules(String serviceName) {
    return getSpanProcessingRulesResponse(serviceName, null)
        .getSpanProcessingRulesServiceResponsesList()
        .get(0)
        .getSpanProcessingRules();
  }

  private SpanProcessingRules getSpanProcessingRulesWithEnv(
      String serviceName, String environment) {
    return getSpanProcessingRulesResponse(serviceName, environment)
        .getSpanProcessingRulesServiceResponsesList()
        .get(0)
        .getSpanProcessingRules();
  }

  private GetSpanProcessingRulesResponse getSpanProcessingRulesResponse(
      String serviceName, String environment) {
    GetSpanProcessingRulesRequest.Builder requestBuilder =
        GetSpanProcessingRulesRequest.newBuilder()
            .addServiceRequests(
                SpanProcessingRulesServiceRequest.newBuilder().setServiceName(serviceName).build());
    if (environment != null) {
      requestBuilder.setEnvironment(environment);
    }
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID, () -> localProcessingConfigStub.getSpanProcessingRules(requestBuilder.build()));
  }

  private static RateLimitConfig buildRateLimitConfig() {
    return RateLimitConfig.newBuilder()
        .setTraceLimitGlobal(
            RateLimit.newBuilder()
                .setFixedWindowLimit(
                    WindowedRateLimit.newBuilder()
                        .setQuantityAllowed(1000)
                        .setWindowDuration(Duration.newBuilder().setSeconds(60).build())
                        .build())
                .build())
        .setTraceLimitPerEndpoint(
            RateLimit.newBuilder()
                .setFixedWindowLimit(
                    WindowedRateLimit.newBuilder()
                        .setQuantityAllowed(100)
                        .setWindowDuration(Duration.newBuilder().setSeconds(60).build())
                        .build())
                .build())
        .setApiEndpointCacheDuration(Duration.newBuilder().setSeconds(600).build())
        .setRateLimitStrategy(RateLimitStrategy.RATE_LIMIT_STRATEGY_DROP)
        .build();
  }

  private static SpanFilter buildEnvironmentFilter(String environment) {
    return SpanFilter.newBuilder()
        .setRelationalSpanFilter(
            RelationalSpanFilterExpression.newBuilder()
                .setField(Field.FIELD_ENVIRONMENT_NAME)
                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQUALS)
                .setRightOperand(SpanFilterValue.newBuilder().setStringValue(environment).build())
                .build())
        .build();
  }

  private static SpanFilter buildServiceNameFilter(String serviceName) {
    return SpanFilter.newBuilder()
        .setRelationalSpanFilter(
            RelationalSpanFilterExpression.newBuilder()
                .setField(Field.FIELD_SERVICE_NAME)
                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                .setRightOperand(SpanFilterValue.newBuilder().setStringValue(serviceName).build())
                .build())
        .build();
  }
}
