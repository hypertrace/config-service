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
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.span.processing.config.service.apinamingrules.DefaultApiNamingRulesManager;
import org.hypertrace.span.processing.config.service.store.ApiNamingRulesConfigStore;
import org.hypertrace.span.processing.config.service.store.ExcludeSpanRulesConfigStore;
import org.hypertrace.span.processing.config.service.utils.TimestampConverter;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleConfig;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleInfo;
import org.hypertrace.span.processing.config.service.v1.ApiSpecBasedConfig;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.Field;
import org.hypertrace.span.processing.config.service.v1.GetAllApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.LogicalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SegmentMatchingBasedConfig;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.SpanFilterValue;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRulesRequest;
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
                new SpanProcessingConfigRequestValidator(),
                this.timestampConverter,
                new DefaultApiNamingRulesManager(
                    new ApiNamingRulesConfigStore(genericStub, this.timestampConverter),
                    this.timestampConverter),
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

  @Test
  void testSegmentBasedApiNamingRules() {
    ApiNamingRuleDetails firstCreatedApiNamingRuleDetails =
        this.spanProcessingConfigServiceStub
            .createApiNamingRule(
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("ruleName1")
                            .setDisabled(true)
                            .setRuleConfig(
                                buildSegmentMatchingBasedConfig(List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter()))
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
                                buildSegmentMatchingBasedConfig(List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails()
            .getRule();

    List<ApiNamingRule> apiNamingRules = getAllApiNamingRules();
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
                                buildSegmentMatchingBasedConfig(List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails()
            .getRule();
    assertEquals("updatedRuleName1", updatedFirstApiNamingRule.getRuleInfo().getName());
    assertFalse(updatedFirstApiNamingRule.getRuleInfo().getDisabled());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedFirstApiNamingRule));

    this.spanProcessingConfigServiceStub.deleteApiNamingRule(
        DeleteApiNamingRuleRequest.newBuilder().setId(firstCreatedApiNamingRule.getId()).build());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(1, apiNamingRules.size());
    assertEquals(secondCreatedApiNamingRule, apiNamingRules.get(0));
  }

  // TODO: remove this test after migration of upstream services and configs
  @Test
  void testApiSpecBasedApiNamingRules() {
    List<ApiNamingRuleDetails> firstTwoCreatedApiNamingRuleDetails =
        this.spanProcessingConfigServiceStub
            .createApiNamingRules(
                CreateApiNamingRulesRequest.newBuilder()
                    .addAllRulesInfo(
                        List.of(
                            ApiNamingRuleInfo.newBuilder()
                                .setName("ruleName1")
                                .setDisabled(true)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        "id1", List.of("regex"), List.of("value")))
                                .setFilter(buildTestFilter())
                                .build(),
                            ApiNamingRuleInfo.newBuilder()
                                .setName("ruleName2")
                                .setDisabled(true)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        "id2", List.of("regex"), List.of("value")))
                                .setFilter(buildTestFilter())
                                .build()))
                    .build())
            .getRulesDetailsList();

    ApiNamingRuleDetails firstCreatedApiNamingRuleDetails =
        firstTwoCreatedApiNamingRuleDetails.get(1);
    ApiNamingRule firstCreatedApiNamingRule = firstTwoCreatedApiNamingRuleDetails.get(1).getRule();
    ApiNamingRule secondCreatedApiNamingRule = firstTwoCreatedApiNamingRuleDetails.get(0).getRule();

    List<ApiNamingRule> apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(secondCreatedApiNamingRule));

    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(
        expectedTimestamp, firstCreatedApiNamingRuleDetails.getMetadata().getCreationTimestamp());
    assertEquals(
        expectedTimestamp,
        firstCreatedApiNamingRuleDetails.getMetadata().getLastUpdatedTimestamp());

    ApiNamingRule thirdCreatedApiNamingRule =
        this.spanProcessingConfigServiceStub
            .createApiNamingRule(
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("ruleName3")
                            .setDisabled(true)
                            .setRuleConfig(
                                buildApiSpecBasedConfig("id3", List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails()
            .getRule();

    apiNamingRules = getAllApiNamingRules();
    assertEquals(3, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(secondCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(thirdCreatedApiNamingRule));

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
                                buildApiSpecBasedConfig("id1", List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails()
            .getRule();
    assertEquals("updatedRuleName1", updatedFirstApiNamingRule.getRuleInfo().getName());
    assertFalse(updatedFirstApiNamingRule.getRuleInfo().getDisabled());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(3, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedFirstApiNamingRule));

    List<ApiNamingRule> updatedSecondAndThirdApiNamingRules =
        this.spanProcessingConfigServiceStub
            .updateApiNamingRules(
                UpdateApiNamingRulesRequest.newBuilder()
                    .addAllRules(
                        List.of(
                            UpdateApiNamingRule.newBuilder()
                                .setId(secondCreatedApiNamingRule.getId())
                                .setName("updatedRuleName2")
                                .setDisabled(false)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        "id2", List.of("regex"), List.of("value")))
                                .setFilter(buildTestFilter())
                                .build(),
                            UpdateApiNamingRule.newBuilder()
                                .setId(thirdCreatedApiNamingRule.getId())
                                .setName("updatedRuleName3")
                                .setDisabled(false)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        "id3", List.of("regex"), List.of("value")))
                                .setFilter(buildTestFilter())
                                .build()))
                    .build())
            .getRulesDetailsList()
            .stream()
            .map(ApiNamingRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(3, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedSecondAndThirdApiNamingRules.get(0)));
    assertTrue(apiNamingRules.contains(updatedSecondAndThirdApiNamingRules.get(1)));

    this.spanProcessingConfigServiceStub.deleteApiNamingRule(
        DeleteApiNamingRuleRequest.newBuilder().setId(firstCreatedApiNamingRule.getId()).build());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedSecondAndThirdApiNamingRules.get(0)));
    assertTrue(apiNamingRules.contains(updatedSecondAndThirdApiNamingRules.get(1)));

    this.spanProcessingConfigServiceStub.deleteApiNamingRules(
        DeleteApiNamingRulesRequest.newBuilder()
            .addAllIds(
                List.of(secondCreatedApiNamingRule.getId(), thirdCreatedApiNamingRule.getId()))
            .build());

    apiNamingRules = getAllApiNamingRules();
    assertTrue(apiNamingRules.isEmpty());
  }

  @Test
  void testApiSpecBasedApiNamingRules_handleDuplication() {
    List<ApiNamingRuleDetails> firstTwoCreatedApiNamingRuleDetails =
        this.spanProcessingConfigServiceStub
            .createApiNamingRules(
                CreateApiNamingRulesRequest.newBuilder()
                    .addAllRulesInfo(
                        List.of(
                            ApiNamingRuleInfo.newBuilder()
                                .setName("ruleName1")
                                .setDisabled(true)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        List.of("id1"), List.of("regex"), List.of("value")))
                                .setFilter(buildTestFilter())
                                .build(),
                            ApiNamingRuleInfo.newBuilder()
                                .setName("ruleName2")
                                .setDisabled(true)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        List.of("id2"), List.of("regex1"), List.of("value1")))
                                .setFilter(buildTestFilter())
                                .build()))
                    .build())
            .getRulesDetailsList();

    ApiNamingRuleDetails firstCreatedApiNamingRuleDetails =
        firstTwoCreatedApiNamingRuleDetails.get(1);
    ApiNamingRule firstCreatedApiNamingRule = firstTwoCreatedApiNamingRuleDetails.get(1).getRule();
    ApiNamingRule secondCreatedApiNamingRule = firstTwoCreatedApiNamingRuleDetails.get(0).getRule();

    List<ApiNamingRule> apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(secondCreatedApiNamingRule));

    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(
        expectedTimestamp, firstCreatedApiNamingRuleDetails.getMetadata().getCreationTimestamp());
    assertEquals(
        expectedTimestamp,
        firstCreatedApiNamingRuleDetails.getMetadata().getLastUpdatedTimestamp());

    ApiNamingRule thirdCreatedApiNamingRule =
        this.spanProcessingConfigServiceStub
            .createApiNamingRules(
                CreateApiNamingRulesRequest.newBuilder()
                    .addAllRulesInfo(
                        List.of(
                            ApiNamingRuleInfo.newBuilder()
                                .setName("ruleName3")
                                .setDisabled(true)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        List.of("id3"), List.of("regex"), List.of("value")))
                                .setFilter(buildTestFilter())
                                .build()))
                    .build())
            .getRulesDetailsList()
            .get(0)
            .getRule();

    apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));
    assertTrue(
        apiNamingRules.contains(
            ApiNamingRule.newBuilder()
                .setId(secondCreatedApiNamingRule.getId())
                .setRuleInfo(
                    ApiNamingRuleInfo.newBuilder()
                        .setName("ruleName1")
                        .setDisabled(true)
                        .setRuleConfig(
                            buildApiSpecBasedConfig(
                                List.of("id1", "id3"), List.of("regex"), List.of("value")))
                        .setFilter(buildTestFilter())
                        .build())
                .build()));

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
                                buildApiSpecBasedConfig(
                                    List.of("id1", "id4"), List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails()
            .getRule();
    assertEquals("updatedRuleName1", updatedFirstApiNamingRule.getRuleInfo().getName());
    assertFalse(updatedFirstApiNamingRule.getRuleInfo().getDisabled());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedFirstApiNamingRule));

    List<ApiNamingRule> updatedFirstAndSecondApiNamingRules =
        this.spanProcessingConfigServiceStub
            .updateApiNamingRules(
                UpdateApiNamingRulesRequest.newBuilder()
                    .addAllRules(
                        List.of(
                            UpdateApiNamingRule.newBuilder()
                                .setId(firstCreatedApiNamingRule.getId())
                                .setName("updatedRuleName1")
                                .setDisabled(false)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        List.of("id1", "id4", "id5"),
                                        List.of("regex"),
                                        List.of("value")))
                                .setFilter(buildTestFilter())
                                .build(),
                            UpdateApiNamingRule.newBuilder()
                                .setId(secondCreatedApiNamingRule.getId())
                                .setName("updatedRuleName2")
                                .setDisabled(false)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        "id3", List.of("regex1"), List.of("value1")))
                                .setFilter(buildTestFilter())
                                .build()))
                    .build())
            .getRulesDetailsList()
            .stream()
            .map(ApiNamingRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedFirstAndSecondApiNamingRules.get(0)));
    assertTrue(apiNamingRules.contains(updatedFirstAndSecondApiNamingRules.get(1)));

    this.spanProcessingConfigServiceStub.deleteApiNamingRule(
        DeleteApiNamingRuleRequest.newBuilder().setId(firstCreatedApiNamingRule.getId()).build());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(1, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedFirstAndSecondApiNamingRules.get(1)));

    this.spanProcessingConfigServiceStub.deleteApiNamingRules(
        DeleteApiNamingRulesRequest.newBuilder()
            .addAllIds(List.of(secondCreatedApiNamingRule.getId()))
            .build());

    apiNamingRules = getAllApiNamingRules();
    assertTrue(apiNamingRules.isEmpty());
  }

  // TODO: remove this test after migration of upstream services and configs
  @Test
  void testApiSpecBasedApiNamingRules_checkNamingRuleMatchWithOlderFormat() {
    // shouldn't match with a naming rule of older format even if identification regexes,replacement
    // values and filter are same
    ApiNamingRuleDetails firstCreatedApiNamingRuleDetails =
        this.spanProcessingConfigServiceStub
            .createApiNamingRule(
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("ruleName1")
                            .setDisabled(true)
                            .setRuleConfig(
                                buildApiSpecBasedConfig("id1", List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter())
                            .build())
                    .build())
            .getRuleDetails();

    ApiNamingRule firstCreatedApiNamingRule = firstCreatedApiNamingRuleDetails.getRule();

    List<ApiNamingRule> apiNamingRules = getAllApiNamingRules();
    assertEquals(1, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));

    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(
        expectedTimestamp, firstCreatedApiNamingRuleDetails.getMetadata().getCreationTimestamp());
    assertEquals(
        expectedTimestamp,
        firstCreatedApiNamingRuleDetails.getMetadata().getLastUpdatedTimestamp());

    ApiNamingRule secondCreatedApiNamingRule =
        this.spanProcessingConfigServiceStub
            .createApiNamingRules(
                CreateApiNamingRulesRequest.newBuilder()
                    .addAllRulesInfo(
                        List.of(
                            ApiNamingRuleInfo.newBuilder()
                                .setName("ruleName2")
                                .setDisabled(true)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        List.of("id3"), List.of("regex"), List.of("value")))
                                .setFilter(buildTestFilter())
                                .build()))
                    .build())
            .getRulesDetailsList()
            .get(0)
            .getRule();

    apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(secondCreatedApiNamingRule));
  }

  private List<ApiNamingRule> getAllApiNamingRules() {
    return this.spanProcessingConfigServiceStub
        .getAllApiNamingRules(GetAllApiNamingRulesRequest.newBuilder().build())
        .getRuleDetailsList()
        .stream()
        .map(ApiNamingRuleDetails::getRule)
        .collect(Collectors.toUnmodifiableList());
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

  private ApiNamingRuleConfig buildSegmentMatchingBasedConfig(
      List<String> regexes, List<String> values) {
    return ApiNamingRuleConfig.newBuilder()
        .setSegmentMatchingBasedConfig(
            SegmentMatchingBasedConfig.newBuilder().addAllRegexes(regexes).addAllValues(values))
        .build();
  }

  private ApiNamingRuleConfig buildApiSpecBasedConfig(
      String id, List<String> regexes, List<String> values) {
    return ApiNamingRuleConfig.newBuilder()
        .setApiSpecBasedConfig(
            ApiSpecBasedConfig.newBuilder()
                .setApiSpecId(id)
                .addAllRegexes(regexes)
                .addAllValues(values))
        .build();
  }

  private ApiNamingRuleConfig buildApiSpecBasedConfig(
      List<String> id, List<String> regexes, List<String> values) {
    return ApiNamingRuleConfig.newBuilder()
        .setApiSpecBasedConfig(
            ApiSpecBasedConfig.newBuilder()
                .addAllApiSpecIds(id)
                .addAllRegexes(regexes)
                .addAllValues(values))
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
