package org.hypertrace.span.processing.config.service;

import static org.hypertrace.span.processing.config.service.v1.Field.FIELD_URL;
import static org.hypertrace.span.processing.config.service.v1.LogicalOperator.LOGICAL_OPERATOR_AND;
import static org.hypertrace.span.processing.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_CONTAINS;
import static org.hypertrace.span.processing.config.service.v1.RuleType.RULE_TYPE_SYSTEM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.Timestamp;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.span.processing.config.service.store.ExcludeSpanRulesConfigStore;
import org.hypertrace.span.processing.config.service.utils.TimestampConverter;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.Field;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.LogicalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.SpanFilterValue;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
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
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    this.mockGenericConfigService
        .addService(
            new SpanProcessingConfigServiceImpl(
                new ExcludeSpanRulesConfigStore(
                    genericStub, this.timestampConverter, configChangeEventGenerator),
                new SpanProcessingConfigRequestValidator(),
                this.timestampConverter,
                buildMockConfig()))
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
    ExcludeSpanRule mockSystemExcludeSpanRule = buildMockSystemExcludeSpanRule(false);
    ExcludeSpanRuleDetails firstCreatedExcludeSpanRuleDetails =
        this.spanProcessingConfigServiceStub
            .createExcludeSpanRule(
                CreateExcludeSpanRuleRequest.newBuilder()
                    .setRuleInfo(
                        ExcludeSpanRuleInfo.newBuilder()
                            .setName("ruleName1")
                            .setDisabled(true)
                            .setFilter(buildTestFilter()))
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
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails()
            .getRule();

    List<ExcludeSpanRule> excludeSpanRules = getAllExcludeSpanRules();
    assertEquals(3, excludeSpanRules.size());
    assertTrue(excludeSpanRules.contains(firstCreatedExcludeSpanRule));
    assertTrue(excludeSpanRules.contains(secondCreatedExcludeSpanRule));
    assertTrue(excludeSpanRules.contains(mockSystemExcludeSpanRule));

    ExcludeSpanRule updatedFirstExcludeSpanRule =
        this.spanProcessingConfigServiceStub
            .updateExcludeSpanRule(
                UpdateExcludeSpanRuleRequest.newBuilder()
                    .setRule(
                        UpdateExcludeSpanRule.newBuilder()
                            .setId(firstCreatedExcludeSpanRule.getId())
                            .setName("updatedRuleName1")
                            .setDisabled(false)
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails()
            .getRule();
    assertEquals("updatedRuleName1", updatedFirstExcludeSpanRule.getRuleInfo().getName());
    assertFalse(updatedFirstExcludeSpanRule.getRuleInfo().getDisabled());

    excludeSpanRules = getAllExcludeSpanRules();
    assertEquals(3, excludeSpanRules.size());
    assertTrue(excludeSpanRules.contains(updatedFirstExcludeSpanRule));

    this.spanProcessingConfigServiceStub.deleteExcludeSpanRule(
        DeleteExcludeSpanRuleRequest.newBuilder()
            .setId(firstCreatedExcludeSpanRule.getId())
            .build());

    excludeSpanRules = getAllExcludeSpanRules();
    assertEquals(2, excludeSpanRules.size());
    assertEquals(secondCreatedExcludeSpanRule, excludeSpanRules.get(0));
    assertEquals(mockSystemExcludeSpanRule, excludeSpanRules.get(1));

    // update system exclude span rule (it does not exist in store as of now)
    ExcludeSpanRule updatedSystemExcludeSpanRule =
        this.spanProcessingConfigServiceStub
            .updateExcludeSpanRule(
                UpdateExcludeSpanRuleRequest.newBuilder()
                    .setRule(
                        UpdateExcludeSpanRule.newBuilder()
                            .setId(mockSystemExcludeSpanRule.getId())
                            .setName(mockSystemExcludeSpanRule.getRuleInfo().getName())
                            .setDisabled(true)
                            .setFilter(mockSystemExcludeSpanRule.getRuleInfo().getFilter()))
                    .build())
            .getRuleDetails()
            .getRule();
    assertEquals(mockSystemExcludeSpanRule.getId(), updatedSystemExcludeSpanRule.getId());
    assertTrue(updatedSystemExcludeSpanRule.getRuleInfo().getDisabled());
    excludeSpanRules = getAllExcludeSpanRules();
    assertEquals(2, excludeSpanRules.size());
    assertEquals(secondCreatedExcludeSpanRule, excludeSpanRules.get(0));
    assertEquals(buildMockSystemExcludeSpanRule(true), excludeSpanRules.get(1));

    // update system exclude span rule (it exists in store now)
    updatedSystemExcludeSpanRule =
        this.spanProcessingConfigServiceStub
            .updateExcludeSpanRule(
                UpdateExcludeSpanRuleRequest.newBuilder()
                    .setRule(
                        UpdateExcludeSpanRule.newBuilder()
                            .setId(mockSystemExcludeSpanRule.getId())
                            .setName(mockSystemExcludeSpanRule.getRuleInfo().getName())
                            .setDisabled(false)
                            .setFilter(mockSystemExcludeSpanRule.getRuleInfo().getFilter()))
                    .build())
            .getRuleDetails()
            .getRule();
    assertEquals(mockSystemExcludeSpanRule.getId(), updatedSystemExcludeSpanRule.getId());
    assertFalse(updatedSystemExcludeSpanRule.getRuleInfo().getDisabled());
    excludeSpanRules = getAllExcludeSpanRules();
    assertEquals(2, excludeSpanRules.size());
    assertEquals(secondCreatedExcludeSpanRule, excludeSpanRules.get(0));
    assertEquals(mockSystemExcludeSpanRule, excludeSpanRules.get(1));

    // throw exception if there is no user or system rule corresponding to the id
    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.spanProcessingConfigServiceStub.updateExcludeSpanRule(
                UpdateExcludeSpanRuleRequest.newBuilder()
                    .setRule(
                        UpdateExcludeSpanRule.newBuilder()
                            .setId("81d6b39a-dca7-4bf0-b98c-d5c5d7ff0a3b")
                            .setName("name")
                            .setDisabled(true)
                            .setFilter(buildTestFilter())
                            .build())
                    .build()));

    // throw exception if we try to update any field other than disabled for system rule
    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.spanProcessingConfigServiceStub.updateExcludeSpanRule(
                UpdateExcludeSpanRuleRequest.newBuilder()
                    .setRule(
                        UpdateExcludeSpanRule.newBuilder()
                            .setId(mockSystemExcludeSpanRule.getId())
                            .setName("name")
                            .setDisabled(true)
                            .setFilter(buildTestFilter())
                            .build())
                    .build()));

    // throw exception if we try to delete a system exclude rule
    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.spanProcessingConfigServiceStub.deleteExcludeSpanRule(
                DeleteExcludeSpanRuleRequest.newBuilder()
                    .setId(mockSystemExcludeSpanRule.getId())
                    .build()));
  }

  private SpanFilter buildTestFilter() {
    return SpanFilter.newBuilder()
        .setRelationalSpanFilter(
            RelationalSpanFilterExpression.newBuilder()
                .setField(Field.FIELD_SERVICE_NAME)
                .setOperator(RELATIONAL_OPERATOR_CONTAINS)
                .setRightOperand(SpanFilterValue.newBuilder().setStringValue("a")))
        .build();
  }

  private List<ExcludeSpanRule> getAllExcludeSpanRules() {
    return this.spanProcessingConfigServiceStub
        .getAllExcludeSpanRules(GetAllExcludeSpanRulesRequest.newBuilder().build())
        .getRuleDetailsList()
        .stream()
        .map(ExcludeSpanRuleDetails::getRule)
        .collect(Collectors.toUnmodifiableList());
  }

  private ExcludeSpanRule buildMockSystemExcludeSpanRule(boolean disabled) {
    return ExcludeSpanRule.newBuilder()
        .setId("70d6b39a-dca7-4bf0-b98c-d5c5d7ff0a3a")
        .setRuleInfo(
            ExcludeSpanRuleInfo.newBuilder()
                .setName("System Exclusion - Health checks")
                .setDisabled(disabled)
                .setType(RULE_TYPE_SYSTEM)
                .setFilter(
                    SpanFilter.newBuilder()
                        .setLogicalSpanFilter(
                            LogicalSpanFilterExpression.newBuilder()
                                .setOperator(LOGICAL_OPERATOR_AND)
                                .addOperands(
                                    SpanFilter.newBuilder()
                                        .setRelationalSpanFilter(
                                            RelationalSpanFilterExpression.newBuilder()
                                                .setOperator(RELATIONAL_OPERATOR_CONTAINS)
                                                .setField(FIELD_URL)
                                                .setRightOperand(
                                                    SpanFilterValue.newBuilder()
                                                        .setStringValue("health")
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  private Config buildMockConfig() {
    return ConfigFactory.parseMap(
        Map.of(
            "span.processing.config.service",
            Map.of(
                "system.exclude.span.rules",
                List.of(
                    Map.of(
                        "id",
                        "70d6b39a-dca7-4bf0-b98c-d5c5d7ff0a3a",
                        "rule_info",
                        Map.of(
                            "name",
                            "System Exclusion - Health checks",
                            "disabled",
                            false,
                            "type",
                            "RULE_TYPE_SYSTEM",
                            "filter",
                            Map.of(
                                "logical_span_filter",
                                Map.of(
                                    "operator",
                                    "LOGICAL_OPERATOR_AND",
                                    "operands",
                                    List.of(
                                        Map.of(
                                            "relational_span_filter",
                                            Map.of(
                                                "field",
                                                "FIELD_URL",
                                                "operator",
                                                "RELATIONAL_OPERATOR_CONTAINS",
                                                "right_operand",
                                                Map.of("string_value", "health"))))))))))));
  }
}
