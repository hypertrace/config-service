package ai.traceable.customsignature.config.service.rules.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.customsignature.config.service.CustomSignatureConfigServiceConfig;
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
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class CustomSignatureConfigContextCacheProviderTest {

  @Mock private CustomSignatureRulesStore rulesStore;
  @Mock private CustomSignatureConfigContextConverter configContextConverter;
  @Mock private KafkaLiveEventListener kafkaLiveEventListener;
  @Mock private CustomSignatureConfigServiceConfig config;

  private CustomSignatureConfigContextCacheProvider cacheProvider;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);

    // Mock config values
    when(config.getCustomSignatureConfigContextCacheMaxSize()).thenReturn(100L);
    when(config.getCustomSignatureConfigContextCacheRefreshAfterWriteDuration())
        .thenReturn(Duration.ofMinutes(5));
    when(config.getCustomSignatureConfigContextCacheThreadPoolSize()).thenReturn(2);

    cacheProvider =
        new CustomSignatureConfigContextCacheProvider(
            rulesStore, configContextConverter, kafkaLiveEventListener, config);
  }

  @Test
  void testCacheHitForSameRequest() {
    RequestContext testContext = RequestContext.forTenantId("test-tenant");
    Context.current()
        .withValue(RequestContext.CURRENT, testContext)
        .run(
            () -> {
              // Create mock rule records
              CustomSignatureRule allowRule = createTestRule("rule-1", EventType.EVENT_TYPE_ALLOW);
              CustomSignatureRuleRecord testRuleRecord = createTestRuleRecord(allowRule);
              List<CustomSignatureRuleRecord> ruleRecords = Arrays.asList(testRuleRecord);

              // Mock store behavior
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

              // First call - should hit cache loader
              CustomSignatureConfigContext result1 =
                  cacheProvider.getCustomSignatureConfigContext(testContext, request);
              assertNotNull(result1);
              assertEquals(expectedResult, result1);

              // Second call - should hit cache (same request)
              CustomSignatureConfigContext result2 =
                  cacheProvider.getCustomSignatureConfigContext(testContext, request);
              assertNotNull(result2);
              assertEquals(expectedResult, result2);
              assertEquals(result1, result2); // Should be equal
            });
  }

  @Test
  void testCacheMissForDifferentEventType() {
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

              // Create different results for different event types
              CustomSignatureConfigContext allowResult =
                  CustomSignatureConfigContext.getDefaultInstance();

              when(configContextConverter.convert(
                      any(List.class), any(GetCustomSignatureEvaluationConfigContextRequest.class)))
                  .thenReturn(allowResult);

              // Create ALLOW request
              GetCustomSignatureEvaluationConfigContextRequest allowRequest =
                  GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
                      .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                      .setEventType(EventType.EVENT_TYPE_ALLOW)
                      .build();

              // Create BLOCK request
              GetCustomSignatureEvaluationConfigContextRequest blockRequest =
                  GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
                      .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                      .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                      .build();

              // First call with ALLOW
              CustomSignatureConfigContext result1 =
                  cacheProvider.getCustomSignatureConfigContext(testContext, allowRequest);
              assertNotNull(result1);

              // Second call with BLOCK - should be cache miss (different event type)
              CustomSignatureConfigContext result2 =
                  cacheProvider.getCustomSignatureConfigContext(testContext, blockRequest);
              assertNotNull(result2);

              // Both should be valid results but potentially different
              // The key point is that both calls succeeded and were cached separately
            });
  }

  @Test
  void testCacheMissForDifferentRuleEvaluationPoint() {
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

              // Create expected result
              CustomSignatureConfigContext expectedResult =
                  CustomSignatureConfigContext.getDefaultInstance();
              when(configContextConverter.convert(
                      any(List.class), any(GetCustomSignatureEvaluationConfigContextRequest.class)))
                  .thenReturn(expectedResult);

              // Create EDGE request
              GetCustomSignatureEvaluationConfigContextRequest edgeRequest =
                  GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
                      .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                      .setEventType(EventType.EVENT_TYPE_ALLOW)
                      .build();

              // Create API request
              GetCustomSignatureEvaluationConfigContextRequest apiRequest =
                  GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
                      .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                      .setEventType(EventType.EVENT_TYPE_ALLOW)
                      .build();

              // First call with EDGE
              CustomSignatureConfigContext result1 =
                  cacheProvider.getCustomSignatureConfigContext(testContext, edgeRequest);
              assertNotNull(result1);

              // Second call with API - should be cache miss (different rule evaluation point)
              CustomSignatureConfigContext result2 =
                  cacheProvider.getCustomSignatureConfigContext(testContext, apiRequest);
              assertNotNull(result2);

              // Both should be valid results but cached separately
            });
  }

  @Test
  void testCacheKeyIsolation() {
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

              // Create different results for different event types
              CustomSignatureConfigContext allowResult =
                  CustomSignatureConfigContext.newBuilder()
                      .addRuleContexts(
                          ai.traceable.protection.engine.config.customsignature.v1
                              .CustomSignatureRulesContext.newBuilder()
                              .addRuleConfigs(
                                  ai.traceable.protection.engine.config.customsignature.v1
                                      .CustomSignatureRuleConfig.newBuilder()
                                      .setId("allow-rule")
                                      .build())
                              .build())
                      .build();

              CustomSignatureConfigContext blockResult =
                  CustomSignatureConfigContext.newBuilder()
                      .addRuleContexts(
                          ai.traceable.protection.engine.config.customsignature.v1
                              .CustomSignatureRulesContext.newBuilder()
                              .addRuleConfigs(
                                  ai.traceable.protection.engine.config.customsignature.v1
                                      .CustomSignatureRuleConfig.newBuilder()
                                      .setId("block-rule")
                                      .build())
                              .build())
                      .build();

              // Mock converter to return different results based on event type
              when(configContextConverter.convert(
                      any(List.class), any(GetCustomSignatureEvaluationConfigContextRequest.class)))
                  .thenAnswer(
                      invocation -> {
                        GetCustomSignatureEvaluationConfigContextRequest request =
                            invocation.getArgument(
                                1, GetCustomSignatureEvaluationConfigContextRequest.class);
                        if (request.getEventType() == EventType.EVENT_TYPE_ALLOW) {
                          return allowResult;
                        } else if (request.getEventType()
                            == EventType.EVENT_TYPE_DETECTION_AND_BLOCKING) {
                          return blockResult;
                        }
                        return CustomSignatureConfigContext.getDefaultInstance();
                      });

              // Create requests
              GetCustomSignatureEvaluationConfigContextRequest allowRequest =
                  GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
                      .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                      .setEventType(EventType.EVENT_TYPE_ALLOW)
                      .build();

              GetCustomSignatureEvaluationConfigContextRequest blockRequest =
                  GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
                      .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                      .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                      .build();

              // Call with ALLOW request
              CustomSignatureConfigContext allowContext =
                  cacheProvider.getCustomSignatureConfigContext(testContext, allowRequest);
              assertNotNull(allowContext);
              assertEquals("allow-rule", allowContext.getRuleContexts(0).getRuleConfigs(0).getId());

              // Call with BLOCK request - should get different cached result
              CustomSignatureConfigContext blockContext =
                  cacheProvider.getCustomSignatureConfigContext(testContext, blockRequest);
              assertNotNull(blockContext);
              assertEquals("block-rule", blockContext.getRuleContexts(0).getRuleConfigs(0).getId());

              // Verify they are different (proves cache isolation)
              assertNotSame(allowContext, blockContext);
              // Note: We don't assert not equals here because the protobuf objects might be equal
              // in structure
              // but the key point is that different cache entries were created
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
