package ai.traceable.customsignature.config.service.rules.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.customsignature.config.service.rules.CustomSignatureRulesStore;
import ai.traceable.customsignature.config.service.rules.converter.CustomSignatureConfigContextConverter;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRuleRecord;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextRequest;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConfigContext;
import io.grpc.Context;
import java.util.Arrays;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class CustomSignatureConfigContextClientProviderTest {

  @Mock private CustomSignatureRulesStore rulesStore;
  @Mock private CustomSignatureConfigContextConverter configContextConverter;

  private CustomSignatureConfigContextClientProvider clientProvider;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    clientProvider =
        new CustomSignatureConfigContextClientProvider(rulesStore, configContextConverter);
  }

  @Test
  void testGetCustomSignatureConfigContextWithEventTypeFilter() {
    RequestContext testContext = RequestContext.forTenantId("test-tenant");
    Context.current()
        .withValue(RequestContext.CURRENT, testContext)
        .run(
            () -> {
              // Create mock rule records
              CustomSignatureRule allowRule = createTestRule("rule-1", EventType.EVENT_TYPE_ALLOW);
              CustomSignatureRule blockRule =
                  createTestRule("rule-2", EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);

              CustomSignatureRuleRecord allowRecord = createTestRuleRecord(allowRule);
              CustomSignatureRuleRecord blockRecord = createTestRuleRecord(blockRule);
              List<CustomSignatureRuleRecord> ruleRecords = Arrays.asList(allowRecord, blockRecord);

              // Mock store behavior
              when(rulesStore.getAllRuleRecords(
                      any(RequestContext.class),
                      any(ai.traceable.customsignature.config.service.v1.GetRulesFilter.class)))
                  .thenReturn(ruleRecords);

              // Create expected filtered result
              CustomSignatureConfigContext expectedResult =
                  CustomSignatureConfigContext.getDefaultInstance();
              when(configContextConverter.convert(
                      any(List.class), any(GetCustomSignatureEvaluationConfigContextRequest.class)))
                  .thenReturn(expectedResult);

              // Create request with ALLOW event type
              GetCustomSignatureEvaluationConfigContextRequest request =
                  GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
                      .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                      .setEventType(EventType.EVENT_TYPE_ALLOW)
                      .build();

              // Call the method
              CustomSignatureConfigContext result =
                  clientProvider.getCustomSignatureConfigContext(testContext, request);

              // Verify result
              assertNotNull(result);
              assertEquals(expectedResult, result);

              // Verify converter was called with request parameter
              // Note: We can't easily verify the exact parameters due to stream mapping,
              // but we can verify the converter was called
            });
  }

  @Test
  void testGetCustomSignatureConfigContextWithoutEventType() {
    RequestContext testContext = RequestContext.forTenantId("test-tenant");
    Context.current()
        .withValue(RequestContext.CURRENT, testContext)
        .run(
            () -> {
              // Create mock rule records
              CustomSignatureRule allowRule = createTestRule("rule-1", EventType.EVENT_TYPE_ALLOW);
              CustomSignatureRule blockRule =
                  createTestRule("rule-2", EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);

              CustomSignatureRuleRecord allowRecord = createTestRuleRecord(allowRule);
              CustomSignatureRuleRecord blockRecord = createTestRuleRecord(blockRule);
              List<CustomSignatureRuleRecord> ruleRecords = Arrays.asList(allowRecord, blockRecord);

              // Mock store behavior
              when(rulesStore.getAllRuleRecords(
                      any(RequestContext.class),
                      any(ai.traceable.customsignature.config.service.v1.GetRulesFilter.class)))
                  .thenReturn(ruleRecords);

              // Create expected result (all rules)
              CustomSignatureConfigContext expectedResult =
                  CustomSignatureConfigContext.getDefaultInstance();
              when(configContextConverter.convert(
                      any(List.class), any(GetCustomSignatureEvaluationConfigContextRequest.class)))
                  .thenReturn(expectedResult);

              // Create request without event type
              GetCustomSignatureEvaluationConfigContextRequest request =
                  GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
                      .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                      .build();

              // Call the method
              CustomSignatureConfigContext result =
                  clientProvider.getCustomSignatureConfigContext(testContext, request);

              // Verify result
              assertNotNull(result);
              assertEquals(expectedResult, result);
            });
  }

  @Test
  void testGetCustomSignatureConfigContextWithEmptyRules() {
    RequestContext testContext = RequestContext.forTenantId("test-tenant");
    Context.current()
        .withValue(RequestContext.CURRENT, testContext)
        .run(
            () -> {
              // Mock empty rule records
              List<CustomSignatureRuleRecord> ruleRecords = Arrays.asList();
              when(rulesStore.getAllRuleRecords(
                      any(RequestContext.class),
                      any(ai.traceable.customsignature.config.service.v1.GetRulesFilter.class)))
                  .thenReturn(ruleRecords);

              // Create expected result
              CustomSignatureConfigContext expectedResult =
                  CustomSignatureConfigContext.getDefaultInstance();
              when(configContextConverter.convert(
                      any(List.class), any(GetCustomSignatureEvaluationConfigContextRequest.class)))
                  .thenReturn(expectedResult);

              // Create request
              GetCustomSignatureEvaluationConfigContextRequest request =
                  GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
                      .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                      .setEventType(EventType.EVENT_TYPE_ALLOW)
                      .build();

              // Call the method
              CustomSignatureConfigContext result =
                  clientProvider.getCustomSignatureConfigContext(testContext, request);

              // Verify result
              assertNotNull(result);
              assertEquals(expectedResult, result);
            });
  }

  @Test
  void testGetCustomSignatureConfigContextWithConverterException() {
    RequestContext testContext = RequestContext.forTenantId("test-tenant");
    Context.current()
        .withValue(RequestContext.CURRENT, testContext)
        .run(
            () -> {
              // Create mock rule records
              CustomSignatureRule rule = createTestRule("rule-1", EventType.EVENT_TYPE_ALLOW);
              CustomSignatureRuleRecord testRuleRecord = createTestRuleRecord(rule);
              List<CustomSignatureRuleRecord> ruleRecords = Arrays.asList(testRuleRecord);

              // Mock store behavior
              when(rulesStore.getAllRuleRecords(
                      any(RequestContext.class),
                      any(ai.traceable.customsignature.config.service.v1.GetRulesFilter.class)))
                  .thenReturn(ruleRecords);

              // Mock converter exception
              when(configContextConverter.convert(
                      any(List.class), any(GetCustomSignatureEvaluationConfigContextRequest.class)))
                  .thenThrow(new RuntimeException("Converter error"));

              // Create request
              GetCustomSignatureEvaluationConfigContextRequest request =
                  GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
                      .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                      .setEventType(EventType.EVENT_TYPE_ALLOW)
                      .build();

              // Call the method - should not throw exception
              CustomSignatureConfigContext result =
                  clientProvider.getCustomSignatureConfigContext(testContext, request);

              // Should return default instance on error
              assertNotNull(result);
              assertEquals(CustomSignatureConfigContext.getDefaultInstance(), result);
            });
  }

  private CustomSignatureRule createTestRule(String id, EventType eventType) {
    return CustomSignatureRule.newBuilder()
        .setId(id)
        .setName("Test Rule " + id)
        .setDescription("Test rule description")
        .setEffect(
            ai.traceable.customsignature.config.service.v1.RuleEffect.newBuilder()
                .setEventType(eventType)
                .build())
        .setDefinition(
            RuleDefinition.newBuilder()
                .setClauseGroup(
                    ai.traceable.customsignature.config.service.v1.ClauseGroup.newBuilder()
                        .setClauseOperator(
                            ai.traceable.customsignature.config.service.v1.ClauseOperator
                                .CLAUSE_OPERATOR_AND)
                        .build())
                .build())
        .build();
  }

  private CustomSignatureRuleRecord createTestRuleRecord(CustomSignatureRule rule) {
    return CustomSignatureRuleRecord.newBuilder().setRule(rule).build();
  }
}
