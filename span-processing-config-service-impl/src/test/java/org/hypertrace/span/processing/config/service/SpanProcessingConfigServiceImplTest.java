package org.hypertrace.span.processing.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.span.processing.config.service.store.ApiNamingRulesConfigStore;
import org.hypertrace.span.processing.config.service.store.ExcludeSpanRulesConfigStore;
import org.hypertrace.span.processing.config.service.utils.TimestampConverter;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleConfig;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleInfo;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.Field;
import org.hypertrace.span.processing.config.service.v1.GetAllApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.RelationalOperator;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SegmentMatchingBasedConfig;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.SpanFilterValue;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.validation.SpanProcessingConfigRequestValidator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SpanProcessingConfigServiceImplTest {
  SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      spanProcessingConfigServiceStub;
  MockGenericConfigService mockGenericConfigService;
  TimestampConverter timestampConverter;

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

    this.timestampConverter = mock(TimestampConverter.class);
    this.mockGenericConfigService
        .addService(
            new SpanProcessingConfigServiceImpl(
                new ExcludeSpanRulesConfigStore(genericStub, this.timestampConverter),
                new ApiNamingRulesConfigStore(genericStub, this.timestampConverter),
                new SpanProcessingConfigRequestValidator(),
                this.timestampConverter))
        .start();

    this.spanProcessingConfigServiceStub =
        SpanProcessingConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());

    when(this.timestampConverter.convert(any()))
        .thenReturn(Timestamp.newBuilder().setSeconds(100).build());
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void testExcludeSpanRulesCrud() {
    ExcludeSpanRuleDetails firstCreatedExcludeSpanRuleDetails =
        this.spanProcessingConfigServiceStub
            .createExcludeSpanRule(
                CreateExcludeSpanRuleRequest.newBuilder()
                    .setRuleInfo(
                        ExcludeSpanRuleInfo.newBuilder()
                            .setName("ruleName1")
                            .setDisabled(true)
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder().setStringValue("a")))))
                    .build())
            .getRuleDetails();
    ExcludeSpanRule firstCreatedExcludeSpanRule = firstCreatedExcludeSpanRuleDetails.getRule();
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(
        expectedTimestamp, firstCreatedExcludeSpanRuleDetails.getMetadata().getCreationTimestamp());
    assertEquals(
        expectedTimestamp,
        firstCreatedExcludeSpanRuleDetails.getMetadata().getLastUpdatedTimestamp());

    ExcludeSpanRule secondCreatedExcludeSpanRule =
        this.spanProcessingConfigServiceStub
            .createExcludeSpanRule(
                CreateExcludeSpanRuleRequest.newBuilder()
                    .setRuleInfo(
                        ExcludeSpanRuleInfo.newBuilder()
                            .setName("ruleName2")
                            .setDisabled(true)
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder().setStringValue("a")))))
                    .build())
            .getRuleDetails()
            .getRule();

    List<ExcludeSpanRule> excludeSpanRules =
        this.spanProcessingConfigServiceStub
            .getAllExcludeSpanRules(GetAllExcludeSpanRulesRequest.newBuilder().build())
            .getRuleDetailsList()
            .stream()
            .map(ExcludeSpanRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(2, excludeSpanRules.size());
    assertTrue(excludeSpanRules.contains(firstCreatedExcludeSpanRule));
    assertTrue(excludeSpanRules.contains(secondCreatedExcludeSpanRule));

    ExcludeSpanRule updatedFirstExcludeSpanRule =
        this.spanProcessingConfigServiceStub
            .updateExcludeSpanRule(
                UpdateExcludeSpanRuleRequest.newBuilder()
                    .setRule(
                        UpdateExcludeSpanRule.newBuilder()
                            .setId(firstCreatedExcludeSpanRule.getId())
                            .setName("updatedRuleName1")
                            .setDisabled(false)
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder().setStringValue("a")))))
                    .build())
            .getRuleDetails()
            .getRule();
    assertEquals("updatedRuleName1", updatedFirstExcludeSpanRule.getRuleInfo().getName());
    assertFalse(updatedFirstExcludeSpanRule.getRuleInfo().getDisabled());

    excludeSpanRules =
        this.spanProcessingConfigServiceStub
            .getAllExcludeSpanRules(GetAllExcludeSpanRulesRequest.newBuilder().build())
            .getRuleDetailsList()
            .stream()
            .map(ExcludeSpanRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(2, excludeSpanRules.size());
    assertTrue(excludeSpanRules.contains(updatedFirstExcludeSpanRule));

    this.spanProcessingConfigServiceStub.deleteExcludeSpanRule(
        DeleteExcludeSpanRuleRequest.newBuilder()
            .setId(firstCreatedExcludeSpanRule.getId())
            .build());

    excludeSpanRules =
        this.spanProcessingConfigServiceStub
            .getAllExcludeSpanRules(GetAllExcludeSpanRulesRequest.newBuilder().build())
            .getRuleDetailsList()
            .stream()
            .map(ExcludeSpanRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(1, excludeSpanRules.size());
    assertEquals(secondCreatedExcludeSpanRule, excludeSpanRules.get(0));
  }

  @Test
  void testApiNamingRulesCrud() {
    ApiNamingRuleDetails firstCreatedApiNamingRuleDetails =
        this.spanProcessingConfigServiceStub
            .createApiNamingRule(
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("ruleName1")
                            .setDisabled(true)
                            .setRuleConfig(
                                ApiNamingRuleConfig.newBuilder()
                                    .setSegmentMatchingBasedConfig(
                                        SegmentMatchingBasedConfig.newBuilder()
                                            .addAllRegexes(List.of("regex"))
                                            .addAllValues(List.of("value"))
                                            .build())
                                    .build())
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder().setStringValue("a")))))
                    .build())
            .getRuleDetails();
    ApiNamingRule firstCreatedApiNamingRule = firstCreatedApiNamingRuleDetails.getRule();
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(
        expectedTimestamp, firstCreatedApiNamingRuleDetails.getMetadata().getCreationTimestamp());
    assertEquals(
        expectedTimestamp,
        firstCreatedApiNamingRuleDetails.getMetadata().getLastUpdatedTimestamp());

    ApiNamingRule secondCreatedApiNamingRule =
        this.spanProcessingConfigServiceStub
            .createApiNamingRule(
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("ruleName2")
                            .setDisabled(true)
                            .setRuleConfig(
                                ApiNamingRuleConfig.newBuilder()
                                    .setSegmentMatchingBasedConfig(
                                        SegmentMatchingBasedConfig.newBuilder()
                                            .addAllRegexes(List.of("regex"))
                                            .addAllValues(List.of("value"))
                                            .build())
                                    .build())
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder().setStringValue("a")))))
                    .build())
            .getRuleDetails()
            .getRule();

    List<ApiNamingRule> apiNamingRules =
        this.spanProcessingConfigServiceStub
            .getAllApiNamingRules(GetAllApiNamingRulesRequest.newBuilder().build())
            .getRuleDetailsList()
            .stream()
            .map(ApiNamingRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(secondCreatedApiNamingRule));

    ApiNamingRule updatedFirstApiNamingRule =
        this.spanProcessingConfigServiceStub
            .updateApiNamingRule(
                UpdateApiNamingRuleRequest.newBuilder()
                    .setRule(
                        UpdateApiNamingRule.newBuilder()
                            .setId(firstCreatedApiNamingRule.getId())
                            .setName("updatedRuleName1")
                            .setDisabled(false)
                            .setRuleConfig(
                                ApiNamingRuleConfig.newBuilder()
                                    .setSegmentMatchingBasedConfig(
                                        SegmentMatchingBasedConfig.newBuilder()
                                            .addAllRegexes(List.of("regex"))
                                            .addAllValues(List.of("value"))
                                            .build())
                                    .build())
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder().setStringValue("a")))))
                    .build())
            .getRuleDetails()
            .getRule();
    assertEquals("updatedRuleName1", updatedFirstApiNamingRule.getRuleInfo().getName());
    assertFalse(updatedFirstApiNamingRule.getRuleInfo().getDisabled());

    apiNamingRules =
        this.spanProcessingConfigServiceStub
            .getAllApiNamingRules(GetAllApiNamingRulesRequest.newBuilder().build())
            .getRuleDetailsList()
            .stream()
            .map(ApiNamingRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedFirstApiNamingRule));

    this.spanProcessingConfigServiceStub.deleteApiNamingRule(
        DeleteApiNamingRuleRequest.newBuilder().setId(firstCreatedApiNamingRule.getId()).build());

    apiNamingRules =
        this.spanProcessingConfigServiceStub
            .getAllApiNamingRules(GetAllApiNamingRulesRequest.newBuilder().build())
            .getRuleDetailsList()
            .stream()
            .map(ApiNamingRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(1, apiNamingRules.size());
    assertEquals(secondCreatedApiNamingRule, apiNamingRules.get(0));
  }
}
