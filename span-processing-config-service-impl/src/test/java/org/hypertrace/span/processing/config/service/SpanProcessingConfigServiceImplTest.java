package org.hypertrace.span.processing.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.span.processing.config.service.store.ApiNamingRulesConfigStore;
import org.hypertrace.span.processing.config.service.store.ExcludeSpanRulesConfigStore;
import org.hypertrace.span.processing.config.service.store.IncludeSpanRulesConfigStore;
import org.hypertrace.span.processing.config.service.store.SamplingConfigsConfigStore;
import org.hypertrace.span.processing.config.service.utils.TimestampConverter;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleConfig;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleInfo;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateIncludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateSamplingConfigRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteIncludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.Field;
import org.hypertrace.span.processing.config.service.v1.GetAllApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllIncludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllSamplingConfigsRequest;
import org.hypertrace.span.processing.config.service.v1.IncludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.IncludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.IncludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.RateLimit;
import org.hypertrace.span.processing.config.service.v1.RateLimitConfig;
import org.hypertrace.span.processing.config.service.v1.RelationalOperator;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SamplingConfig;
import org.hypertrace.span.processing.config.service.v1.SamplingConfigDetails;
import org.hypertrace.span.processing.config.service.v1.SamplingConfigInfo;
import org.hypertrace.span.processing.config.service.v1.SegmentMatchingBasedConfig;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.SpanFilterValue;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateIncludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.UpdateIncludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateSamplingConfig;
import org.hypertrace.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import org.hypertrace.span.processing.config.service.v1.WindowedRateLimit;
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
                new IncludeSpanRulesConfigStore(genericStub, this.timestampConverter),
                new ApiNamingRulesConfigStore(genericStub, this.timestampConverter),
                new SamplingConfigsConfigStore(genericStub, this.timestampConverter),
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
  void testIncludeSpanRulesCrud() {
    IncludeSpanRuleDetails firstCreatedIncludeSpanRuleDetails =
        this.spanProcessingConfigServiceStub
            .createIncludeSpanRule(
                CreateIncludeSpanRuleRequest.newBuilder()
                    .setRuleInfo(
                        IncludeSpanRuleInfo.newBuilder()
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
    IncludeSpanRule firstCreatedIncludeSpanRule = firstCreatedIncludeSpanRuleDetails.getRule();
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(
        expectedTimestamp, firstCreatedIncludeSpanRuleDetails.getMetadata().getCreationTimestamp());
    assertEquals(
        expectedTimestamp,
        firstCreatedIncludeSpanRuleDetails.getMetadata().getLastUpdatedTimestamp());

    IncludeSpanRule secondCreatedIncludeSpanRule =
        this.spanProcessingConfigServiceStub
            .createIncludeSpanRule(
                CreateIncludeSpanRuleRequest.newBuilder()
                    .setRuleInfo(
                        IncludeSpanRuleInfo.newBuilder()
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

    List<IncludeSpanRule> includeSpanRules =
        this.spanProcessingConfigServiceStub
            .getAllIncludeSpanRules(GetAllIncludeSpanRulesRequest.newBuilder().build())
            .getRuleDetailsList()
            .stream()
            .map(IncludeSpanRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(2, includeSpanRules.size());
    assertTrue(includeSpanRules.contains(firstCreatedIncludeSpanRule));
    assertTrue(includeSpanRules.contains(secondCreatedIncludeSpanRule));

    IncludeSpanRule updatedFirstIncludeSpanRule =
        this.spanProcessingConfigServiceStub
            .updateIncludeSpanRule(
                UpdateIncludeSpanRuleRequest.newBuilder()
                    .setRule(
                        UpdateIncludeSpanRule.newBuilder()
                            .setId(firstCreatedIncludeSpanRule.getId())
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
    assertEquals("updatedRuleName1", updatedFirstIncludeSpanRule.getRuleInfo().getName());
    assertFalse(updatedFirstIncludeSpanRule.getRuleInfo().getDisabled());

    includeSpanRules =
        this.spanProcessingConfigServiceStub
            .getAllIncludeSpanRules(GetAllIncludeSpanRulesRequest.newBuilder().build())
            .getRuleDetailsList()
            .stream()
            .map(IncludeSpanRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(2, includeSpanRules.size());
    assertTrue(includeSpanRules.contains(updatedFirstIncludeSpanRule));

    this.spanProcessingConfigServiceStub.deleteIncludeSpanRule(
        DeleteIncludeSpanRuleRequest.newBuilder()
            .setId(firstCreatedIncludeSpanRule.getId())
            .build());

    includeSpanRules =
        this.spanProcessingConfigServiceStub
            .getAllIncludeSpanRules(GetAllIncludeSpanRulesRequest.newBuilder().build())
            .getRuleDetailsList()
            .stream()
            .map(IncludeSpanRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(1, includeSpanRules.size());
    assertEquals(secondCreatedIncludeSpanRule, includeSpanRules.get(0));
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

  @Test
  void testSamplingConfigsCrud() {
    SamplingConfigDetails firstCreatedSamplingConfigDetails =
        this.spanProcessingConfigServiceStub
            .createSamplingConfig(
                CreateSamplingConfigRequest.newBuilder()
                    .setSamplingConfigInfo(
                        SamplingConfigInfo.newBuilder()
                            .setRateLimitConfig(
                                RateLimitConfig.newBuilder()
                                    .setTraceLimitGlobal(
                                        RateLimit.newBuilder()
                                            .setFixedWindowLimit(
                                                WindowedRateLimit.newBuilder()
                                                    .setQuantityAllowed(100)
                                                    .setWindowDuration(
                                                        Duration.newBuilder()
                                                            .setSeconds(60)
                                                            .build())
                                                    .build())
                                            .build())
                                    .setTraceLimitPerEndpoint(
                                        RateLimit.newBuilder()
                                            .setFixedWindowLimit(
                                                WindowedRateLimit.newBuilder()
                                                    .setQuantityAllowed(100)
                                                    .setWindowDuration(
                                                        Duration.newBuilder()
                                                            .setSeconds(60)
                                                            .build())
                                                    .build())
                                            .build())
                                    .setApiEndpointCacheDuration(
                                        Duration.newBuilder().setSeconds(100).setNanos(100).build())
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
            .getSamplingConfigDetails();
    SamplingConfig firstCreatedSamplingConfig =
        firstCreatedSamplingConfigDetails.getSamplingConfig();
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(
        expectedTimestamp, firstCreatedSamplingConfigDetails.getMetadata().getCreationTimestamp());
    assertEquals(
        expectedTimestamp,
        firstCreatedSamplingConfigDetails.getMetadata().getLastUpdatedTimestamp());

    SamplingConfig secondCreatedSamplingConfig =
        this.spanProcessingConfigServiceStub
            .createSamplingConfig(
                CreateSamplingConfigRequest.newBuilder()
                    .setSamplingConfigInfo(
                        SamplingConfigInfo.newBuilder()
                            .setRateLimitConfig(
                                RateLimitConfig.newBuilder()
                                    .setTraceLimitGlobal(
                                        RateLimit.newBuilder()
                                            .setFixedWindowLimit(
                                                WindowedRateLimit.newBuilder()
                                                    .setQuantityAllowed(200)
                                                    .setWindowDuration(
                                                        Duration.newBuilder()
                                                            .setSeconds(60)
                                                            .build())
                                                    .build())
                                            .build())
                                    .setTraceLimitPerEndpoint(
                                        RateLimit.newBuilder()
                                            .setFixedWindowLimit(
                                                WindowedRateLimit.newBuilder()
                                                    .setQuantityAllowed(200)
                                                    .setWindowDuration(
                                                        Duration.newBuilder()
                                                            .setSeconds(60)
                                                            .build())
                                                    .build())
                                            .build())
                                    .setApiEndpointCacheDuration(
                                        Duration.newBuilder().setSeconds(100).setNanos(100).build())
                                    .build())
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder().setStringValue("b")))))
                    .build())
            .getSamplingConfigDetails()
            .getSamplingConfig();

    List<SamplingConfig> samplingConfigs =
        this.spanProcessingConfigServiceStub
            .getAllSamplingConfigs(GetAllSamplingConfigsRequest.newBuilder().build())
            .getSamplingConfigDetailsList()
            .stream()
            .map(SamplingConfigDetails::getSamplingConfig)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(2, samplingConfigs.size());
    assertTrue(samplingConfigs.contains(firstCreatedSamplingConfig));
    assertTrue(samplingConfigs.contains(secondCreatedSamplingConfig));

    SamplingConfig updatedFirstSamplingConfig =
        this.spanProcessingConfigServiceStub
            .updateSamplingConfig(
                UpdateSamplingConfigRequest.newBuilder()
                    .setSamplingConfig(
                        UpdateSamplingConfig.newBuilder()
                            .setId(firstCreatedSamplingConfig.getId())
                            .setRateLimitConfig(
                                RateLimitConfig.newBuilder()
                                    .setTraceLimitGlobal(
                                        RateLimit.newBuilder()
                                            .setFixedWindowLimit(
                                                WindowedRateLimit.newBuilder()
                                                    .setQuantityAllowed(300)
                                                    .setWindowDuration(
                                                        Duration.newBuilder()
                                                            .setSeconds(60)
                                                            .build())
                                                    .build())
                                            .build())
                                    .setTraceLimitPerEndpoint(
                                        RateLimit.newBuilder()
                                            .setFixedWindowLimit(
                                                WindowedRateLimit.newBuilder()
                                                    .setQuantityAllowed(100)
                                                    .setWindowDuration(
                                                        Duration.newBuilder()
                                                            .setSeconds(60)
                                                            .build())
                                                    .build())
                                            .build())
                                    .setApiEndpointCacheDuration(
                                        Duration.newBuilder().setSeconds(100).setNanos(100).build())
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
            .getSamplingConfigDetails()
            .getSamplingConfig();
    assertEquals(
        RateLimitConfig.newBuilder()
            .setTraceLimitGlobal(
                RateLimit.newBuilder()
                    .setFixedWindowLimit(
                        WindowedRateLimit.newBuilder()
                            .setQuantityAllowed(300)
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
            .setApiEndpointCacheDuration(
                Duration.newBuilder().setSeconds(100).setNanos(100).build())
            .build(),
        updatedFirstSamplingConfig.getSamplingConfigInfo().getRateLimitConfig());

    samplingConfigs =
        this.spanProcessingConfigServiceStub
            .getAllSamplingConfigs(GetAllSamplingConfigsRequest.newBuilder().build())
            .getSamplingConfigDetailsList()
            .stream()
            .map(SamplingConfigDetails::getSamplingConfig)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(2, samplingConfigs.size());
    assertTrue(samplingConfigs.contains(updatedFirstSamplingConfig));

    this.spanProcessingConfigServiceStub.deleteSamplingConfig(
        DeleteSamplingConfigRequest.newBuilder().setId(firstCreatedSamplingConfig.getId()).build());

    samplingConfigs =
        this.spanProcessingConfigServiceStub
            .getAllSamplingConfigs(GetAllSamplingConfigsRequest.newBuilder().build())
            .getSamplingConfigDetailsList()
            .stream()
            .map(SamplingConfigDetails::getSamplingConfig)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(1, samplingConfigs.size());
    assertEquals(secondCreatedSamplingConfig, samplingConfigs.get(0));
  }
}
