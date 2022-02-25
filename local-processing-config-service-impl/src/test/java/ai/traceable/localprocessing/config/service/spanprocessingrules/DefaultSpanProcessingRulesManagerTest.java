package ai.traceable.localprocessing.config.service.spanprocessingrules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRule;
import ai.traceable.localprocessing.config.service.v1.GetSpanProcessingRulesRequest;
import ai.traceable.localprocessing.config.service.v1.GetSpanProcessingRulesResponse;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRules;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRulesServiceRequest;
import ai.traceable.localprocessing.config.service.v1.SpanProcessingRulesServiceResponse;
import java.util.List;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultSpanProcessingRulesManagerTest {

  private SpanProcessingRulesManager spanProcessingRulesManager;
  private UuidGenerator uuidGenerator;
  private SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      configServiceBlockingStub;

  @BeforeEach
  void setup() {
    configServiceBlockingStub =
        mock(SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub.class);
    uuidGenerator = new UuidGenerator();
    spanProcessingRulesManager =
        new DefaultSpanProcessingRulesManager(configServiceBlockingStub, uuidGenerator);
  }

  @Test
  void testGetAllExcludeSpanRules() {
    when(configServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(SpanProcessingRulesManagerTestUtils.buildGetAllExcludeSpanRulesResponse(false));

    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
            GetSpanProcessingRulesRequest.newBuilder()
                .addServiceRequests(
                    SpanProcessingRulesServiceRequest.newBuilder()
                        .setServiceName("service1")
                        .build())
                .build());
    ExcludeSpanProcessingRule expectedExcludeSpanProcessingRule =
        SpanProcessingRulesManagerTestUtils.buildExpectedExcludeSpanProcessingRule();

    String hash =
        uuidGenerator.generateId(
            SpanProcessingRules.newBuilder()
                .addAllExcludeSpanRules(List.of(expectedExcludeSpanProcessingRule))
                .build());
    SpanProcessingRules expectedSpanProcessingRules =
        SpanProcessingRules.newBuilder()
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
    when(configServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(
            SpanProcessingRulesManagerTestUtils
                .buildGetAllExcludeSpanRulesResponseEnvironmentFilter());
    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
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

    SpanProcessingRules expectedSpanProcessingRules = SpanProcessingRules.newBuilder().build();
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
        SpanProcessingRulesManagerTestUtils
            .buildExpectedExcludeSpanProcessingRuleServiceNamesAndEnvironmentsProcessed();
    expectedSpanProcessingRules =
        SpanProcessingRules.newBuilder()
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
    when(configServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(
            SpanProcessingRulesManagerTestUtils
                .buildGetAllExcludeSpanRulesResponseServiceNameFilter());

    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
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

    SpanProcessingRules expectedSpanProcessingRulesFirst =
        SpanProcessingRules.newBuilder()
            .addAllExcludeSpanRules(excludeSpanProcessingRulesFirst)
            .build();
    SpanProcessingRules expectedSpanProcessingRulesSecond =
        SpanProcessingRules.newBuilder()
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
    when(configServiceBlockingStub.getAllExcludeSpanRules(any()))
        .thenReturn(SpanProcessingRulesManagerTestUtils.buildGetAllExcludeSpanRulesResponse(true));
    GetSpanProcessingRulesResponse getSpanProcessingRulesResponse =
        spanProcessingRulesManager.getSpanProcessingRulesResponse(
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

    SpanProcessingRules expectedSpanProcessingRules = SpanProcessingRules.newBuilder().build();
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
}
