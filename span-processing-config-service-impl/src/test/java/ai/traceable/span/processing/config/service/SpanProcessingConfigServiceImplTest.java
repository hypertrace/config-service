package ai.traceable.span.processing.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.span.processing.config.service.store.SpanProcessingRulesConfigStore;
import ai.traceable.span.processing.config.service.v1.CreateSpanProcessingRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSpanProcessingRuleRequest;
import ai.traceable.span.processing.config.service.v1.ExcludeSpanRule;
import ai.traceable.span.processing.config.service.v1.Field;
import ai.traceable.span.processing.config.service.v1.Filter;
import ai.traceable.span.processing.config.service.v1.FilterValue;
import ai.traceable.span.processing.config.service.v1.GetAllSpanProcessingRulesRequest;
import ai.traceable.span.processing.config.service.v1.RelationalFilterExpression;
import ai.traceable.span.processing.config.service.v1.RelationalOperator;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import ai.traceable.span.processing.config.service.v1.SpanProcessingRule;
import ai.traceable.span.processing.config.service.v1.SpanProcessingRuleInfo;
import ai.traceable.span.processing.config.service.v1.UpdateExcludeSpanRule;
import ai.traceable.span.processing.config.service.v1.UpdateSpanProcessingRule;
import ai.traceable.span.processing.config.service.v1.UpdateSpanProcessingRulesRequest;
import ai.traceable.span.processing.config.service.validation.SpanProcessingConfigRequestValidator;
import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SpanProcessingConfigServiceImplTest {
  SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      spanProcessingConfigServiceStub;
  MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockUpsertAll();

    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());

    this.mockGenericConfigService
        .addService(
            new SpanProcessingConfigServiceImpl(
                new SpanProcessingRulesConfigStore(genericStub),
                new SpanProcessingConfigRequestValidator(),
                new UuidGenerator()))
        .start();

    this.spanProcessingConfigServiceStub =
        SpanProcessingConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void testCrud() {
    SpanProcessingRule firstCreatedSpanProcessingRule =
        this.spanProcessingConfigServiceStub
            .createSpanProcessingRule(
                CreateSpanProcessingRuleRequest.newBuilder()
                    .setRuleInfo(
                        SpanProcessingRuleInfo.newBuilder()
                            .setName("ruleName1")
                            .setExcludeSpanRule(
                                ExcludeSpanRule.newBuilder()
                                    .setFilter(
                                        Filter.newBuilder()
                                            .setRelationalFilter(
                                                RelationalFilterExpression.newBuilder()
                                                    .setField(Field.FIELD_SERVICE_NAME)
                                                    .setOperator(
                                                        RelationalOperator
                                                            .RELATIONAL_OPERATOR_CONTAINS)
                                                    .setRightOperand(
                                                        FilterValue.newBuilder()
                                                            .setStringValue("a"))))))
                    .build())
            .getRule();

    SpanProcessingRule secondCreatedSpanProcessingRule =
        this.spanProcessingConfigServiceStub
            .createSpanProcessingRule(
                CreateSpanProcessingRuleRequest.newBuilder()
                    .setRuleInfo(
                        SpanProcessingRuleInfo.newBuilder()
                            .setName("ruleName2")
                            .setExcludeSpanRule(
                                ExcludeSpanRule.newBuilder()
                                    .setFilter(
                                        Filter.newBuilder()
                                            .setRelationalFilter(
                                                RelationalFilterExpression.newBuilder()
                                                    .setField(Field.FIELD_SERVICE_NAME)
                                                    .setOperator(
                                                        RelationalOperator
                                                            .RELATIONAL_OPERATOR_CONTAINS)
                                                    .setRightOperand(
                                                        FilterValue.newBuilder()
                                                            .setStringValue("a"))))))
                    .build())
            .getRule();

    List<SpanProcessingRule> spanProcessingRules =
        this.spanProcessingConfigServiceStub
            .getAllSpanProcessingRules(GetAllSpanProcessingRulesRequest.newBuilder().build())
            .getRulesList();
    assertEquals(2, spanProcessingRules.size());
    assertTrue(spanProcessingRules.contains(firstCreatedSpanProcessingRule));
    assertTrue(spanProcessingRules.contains(secondCreatedSpanProcessingRule));

    SpanProcessingRule updatedFirstSpanProcessingRule =
        this.spanProcessingConfigServiceStub
            .updateSpanProcessingRules(
                UpdateSpanProcessingRulesRequest.newBuilder()
                    .addRules(
                        UpdateSpanProcessingRule.newBuilder()
                            .setId(firstCreatedSpanProcessingRule.getId())
                            .setName("updatedRuleName1")
                            .setExcludeSpanRule(
                                UpdateExcludeSpanRule.newBuilder()
                                    .setFilter(
                                        Filter.newBuilder()
                                            .setRelationalFilter(
                                                RelationalFilterExpression.newBuilder()
                                                    .setField(Field.FIELD_SERVICE_NAME)
                                                    .setOperator(
                                                        RelationalOperator
                                                            .RELATIONAL_OPERATOR_CONTAINS)
                                                    .setRightOperand(
                                                        FilterValue.newBuilder()
                                                            .setStringValue("a"))))))
                    .build())
            .getRules(0);
    assertEquals("updatedRuleName1", updatedFirstSpanProcessingRule.getRuleInfo().getName());

    spanProcessingRules =
        this.spanProcessingConfigServiceStub
            .getAllSpanProcessingRules(GetAllSpanProcessingRulesRequest.newBuilder().build())
            .getRulesList();
    assertEquals(2, spanProcessingRules.size());
    assertTrue(spanProcessingRules.contains(updatedFirstSpanProcessingRule));

    this.spanProcessingConfigServiceStub.deleteSpanProcessingRule(
        DeleteSpanProcessingRuleRequest.newBuilder()
            .setId(firstCreatedSpanProcessingRule.getId())
            .build());

    spanProcessingRules =
        this.spanProcessingConfigServiceStub
            .getAllSpanProcessingRules(GetAllSpanProcessingRulesRequest.newBuilder().build())
            .getRulesList();
    assertEquals(1, spanProcessingRules.size());
    assertEquals(secondCreatedSpanProcessingRule, spanProcessingRules.get(0));
  }
}
