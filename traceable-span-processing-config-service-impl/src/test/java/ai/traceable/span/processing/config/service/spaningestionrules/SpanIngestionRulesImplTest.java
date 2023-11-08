package ai.traceable.span.processing.config.service.spaningestionrules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.ObjectDiffer;
import ai.traceable.config.utils.RankCalculator;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.span.processing.config.service.SpanProcessingConfigServiceImpl;
import ai.traceable.span.processing.config.service.apinamingrules.ApiNamingRulesManager;
import ai.traceable.span.processing.config.service.impl.v1.PersistedKeyValueRetentionRule;
import ai.traceable.span.processing.config.service.protectionspanrules.ProtectionSpanRulesManager;
import ai.traceable.span.processing.config.service.samplingconfigs.SamplingConfigManager;
import ai.traceable.span.processing.config.service.servicenaming.ServiceNamingRulesManager;
import ai.traceable.span.processing.config.service.store.DefaultProtectionSpanRuleEvaluationStatusConfigStore;
import ai.traceable.span.processing.config.service.v1.CreateSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.GetSpanIngestionConfigRequest;
import ai.traceable.span.processing.config.service.v1.IngestionStage;
import ai.traceable.span.processing.config.service.v1.KeyValueRetentionRule;
import ai.traceable.span.processing.config.service.v1.KeyValueRetentionRuleData;
import ai.traceable.span.processing.config.service.v1.Predicate;
import ai.traceable.span.processing.config.service.v1.RankSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.RetentionAction;
import ai.traceable.span.processing.config.service.v1.SpanIngestionConfigFilter;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import ai.traceable.span.processing.config.service.v1.StringPredicate;
import ai.traceable.span.processing.config.service.v1.UpdateSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.validation.SpanProcessingConfigRequestValidator;
import com.google.protobuf.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SpanIngestionRulesImplTest {

  private static final GetSpanIngestionConfigRequest VALID_GET_CONFIG_REQUEST =
      GetSpanIngestionConfigRequest.newBuilder()
          .setStage(IngestionStage.INGESTION_STAGE_QUERY_STORE_PERSISTENCE)
          .build();

  @Mock Clock clock;
  TimestampConverter timestampConverter;
  SpanIngestionRuleBuilder ruleBuilder;
  RankCalculator<PersistedKeyValueRetentionRule, String> rankCalculator;
  ObjectDiffer objectDiffer;

  private MockGenericConfigService mockGenericConfigService;
  private SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      spanProcessingConfigServiceStub;

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockUpsertAll();

    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    ServiceNamingRulesManager mockServiceNamingManager = mock(ServiceNamingRulesManager.class);
    SpanIngestionRequestValidator validator = new SpanIngestionRequestValidator();
    this.rankCalculator =
        new RankCalculator<>(
            new RankCalculator.RankConfig<>(
                PersistedKeyValueRetentionRule::getRank,
                PersistedKeyValueRetentionRule::getId,
                (rule, rank) -> rule.toBuilder().setRank(rank).build()));
    this.objectDiffer = new ObjectDiffer();
    this.ruleBuilder = new SpanIngestionRuleBuilder(new UuidGenerator());
    this.timestampConverter = new TimestampConverter();
    SpanIngestionRulesConfigStore ruleStore =
        new SpanIngestionRulesConfigStore(
            genericStub, configChangeEventGenerator, rankCalculator, clock, timestampConverter);
    SpanIngestionRulesManager spanIngestionRuleManager =
        new SpanIngestionRulesManagerImpl(
            ruleStore, validator, ruleBuilder, rankCalculator, objectDiffer);

    this.mockGenericConfigService
        .addService(
            new SpanProcessingConfigServiceImpl(
                mock(SpanProcessingConfigRequestValidator.class),
                mock(SamplingConfigManager.class),
                mock(ApiNamingRulesManager.class),
                mock(ProtectionSpanRulesManager.class),
                mock(DefaultProtectionSpanRuleEvaluationStatusConfigStore.class),
                mockServiceNamingManager,
                spanIngestionRuleManager))
        .start();

    this.spanProcessingConfigServiceStub =
        SpanProcessingConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
  }

  @Test
  void testDifferentCrudOperations() {
    // test create and update
    KeyValueRetentionRule firstCreated =
        this.spanProcessingConfigServiceStub
            .createSpanIngestionRule(
                CreateSpanIngestionRuleRequest.newBuilder()
                    .setStage(IngestionStage.INGESTION_STAGE_QUERY_STORE_PERSISTENCE)
                    .setRequestHeaderRule(
                        KeyValueRetentionRuleData.newBuilder()
                            .setAction(
                                RetentionAction.newBuilder()
                                    .setRetain(RetentionAction.RetainAction.getDefaultInstance()))
                            .setPredicate(
                                Predicate.newBuilder()
                                    .setTargetKeyPredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(StringPredicate.Operator.OPERATOR_EQUALS)
                                            .setValue("header-1"))))
                    .build())
            .getRule();

    assertEquals(1, this.getRequestHeaderRules().size());
    assertEquals(firstCreated, this.getRequestHeaderRules().get(0));

    KeyValueRetentionRule responseHeaderRule =
        this.spanProcessingConfigServiceStub
            .createSpanIngestionRule(
                CreateSpanIngestionRuleRequest.newBuilder()
                    .setStage(IngestionStage.INGESTION_STAGE_QUERY_STORE_PERSISTENCE)
                    .setResponseHeaderRule(
                        KeyValueRetentionRuleData.newBuilder()
                            .setAction(
                                RetentionAction.newBuilder()
                                    .setRetain(RetentionAction.RetainAction.getDefaultInstance()))
                            .setPredicate(
                                Predicate.newBuilder()
                                    .setTargetKeyPredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(StringPredicate.Operator.OPERATOR_EQUALS)
                                            .setValue("response-header-1"))))
                    .build())
            .getRule();

    assertEquals(
        responseHeaderRule,
        this.spanProcessingConfigServiceStub
            .getSpanIngestionConfig(VALID_GET_CONFIG_REQUEST)
            .getResponseHeaderRuleset()
            .getRules(0));

    KeyValueRetentionRule firstUpdated =
        this.spanProcessingConfigServiceStub
            .updateSpanIngestionRule(
                UpdateSpanIngestionRuleRequest.newBuilder()
                    .setId(firstCreated.getId())
                    .setKeyValueRetentionRuleData(
                        firstCreated.getData().toBuilder()
                            .setPredicate(
                                Predicate.newBuilder()
                                    .setTargetKeyPredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(
                                                StringPredicate.Operator.OPERATOR_MATCHES_REGEX)
                                            .setValue("header-updated")))
                            .setExpiration(Timestamp.newBuilder().setSeconds(1913807000)))
                    .build())
            .getRule();

    assertEquals(
        "header-updated", firstUpdated.getData().getPredicate().getTargetKeyPredicate().getValue());
    assertEquals(
        StringPredicate.Operator.OPERATOR_MATCHES_REGEX,
        firstUpdated.getData().getPredicate().getTargetKeyPredicate().getOperator());
    assertEquals(1913807000, firstUpdated.getData().getExpiration().getSeconds());

    KeyValueRetentionRule secondCreated =
        this.spanProcessingConfigServiceStub
            .createSpanIngestionRule(
                CreateSpanIngestionRuleRequest.newBuilder()
                    .setStage(IngestionStage.INGESTION_STAGE_QUERY_STORE_PERSISTENCE)
                    .setRequestHeaderRule(
                        KeyValueRetentionRuleData.newBuilder()
                            .setAction(
                                RetentionAction.newBuilder()
                                    .setRetain(RetentionAction.RetainAction.getDefaultInstance()))
                            .setPredicate(
                                Predicate.newBuilder()
                                    .setTargetKeyPredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(StringPredicate.Operator.OPERATOR_EQUALS)
                                            .setValue("header-2")))
                            .setExpiration(Timestamp.newBuilder().setSeconds(1913807880)))
                    .build())
            .getRule();

    assertEquals(secondCreated, this.getRequestHeaderRules().get(1));
    assertEquals(List.of(firstUpdated, secondCreated), this.getRequestHeaderRules());

    // test ranking operation
    this.spanProcessingConfigServiceStub.rankSpanIngestionRule(
        RankSpanIngestionRuleRequest.newBuilder().setIdToUpdate(secondCreated.getId()).build());
    assertEquals(List.of(secondCreated, firstUpdated), this.getRequestHeaderRules());

    // test delete
    this.spanProcessingConfigServiceStub.deleteSpanIngestionRule(
        DeleteSpanIngestionRuleRequest.newBuilder().setId(firstUpdated.getId()).build());
    assertEquals(secondCreated, this.getRequestHeaderRules().get(0));

    // test expire rule filter
    // when filter for expired is set to true -- return only expired rules
    GetSpanIngestionConfigRequest request =
        GetSpanIngestionConfigRequest.newBuilder()
            .setStage(IngestionStage.INGESTION_STAGE_QUERY_STORE_PERSISTENCE)
            .setFilter(SpanIngestionConfigFilter.newBuilder().setIsRuleExpired(true))
            .build();

    when(clock.instant()).thenReturn(Instant.now());
    KeyValueRetentionRule createdRule =
        this.spanProcessingConfigServiceStub
            .createSpanIngestionRule(
                CreateSpanIngestionRuleRequest.newBuilder()
                    .setStage(IngestionStage.INGESTION_STAGE_QUERY_STORE_PERSISTENCE)
                    .setRequestHeaderRule(
                        KeyValueRetentionRuleData.newBuilder()
                            .setAction(
                                RetentionAction.newBuilder()
                                    .setRetain(RetentionAction.RetainAction.getDefaultInstance()))
                            .setPredicate(
                                Predicate.newBuilder()
                                    .setTargetKeyPredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(StringPredicate.Operator.OPERATOR_EQUALS)
                                            .setValue("header-1")))
                            .setExpiration(Timestamp.newBuilder().setSeconds(1692537480)))
                    .build())
            .getRule();

    assertEquals(
        1,
        this.spanProcessingConfigServiceStub
            .getSpanIngestionConfig(request)
            .getRequestHeaderRuleset()
            .getRulesList()
            .size());
    assertEquals(
        createdRule,
        this.spanProcessingConfigServiceStub
            .getSpanIngestionConfig(request)
            .getRequestHeaderRuleset()
            .getRules(0));

    // test expire rule filter
    // when filter for expired is set to false -- return only non-expired rules
    request =
        GetSpanIngestionConfigRequest.newBuilder()
            .setStage(IngestionStage.INGESTION_STAGE_QUERY_STORE_PERSISTENCE)
            .setFilter(SpanIngestionConfigFilter.newBuilder().setIsRuleExpired(false))
            .build();

    assertEquals(
        1,
        this.spanProcessingConfigServiceStub
            .getSpanIngestionConfig(request)
            .getRequestHeaderRuleset()
            .getRulesList()
            .size());
    assertEquals(
        secondCreated,
        this.spanProcessingConfigServiceStub
            .getSpanIngestionConfig(request)
            .getRequestHeaderRuleset()
            .getRules(0));
  }

  @Test
  void multipleCrudOperationOnDifferentRuleTypes() {
    for (int i = 0; i < 5; i++) {
      this.spanProcessingConfigServiceStub.createSpanIngestionRule(
          CreateSpanIngestionRuleRequest.newBuilder()
              .setStage(IngestionStage.INGESTION_STAGE_QUERY_STORE_PERSISTENCE)
              .setRequestHeaderRule(
                  KeyValueRetentionRuleData.newBuilder()
                      .setAction(
                          RetentionAction.newBuilder()
                              .setRetain(RetentionAction.RetainAction.getDefaultInstance()))
                      .setPredicate(
                          Predicate.newBuilder()
                              .setTargetKeyPredicate(
                                  StringPredicate.newBuilder()
                                      .setOperator(StringPredicate.Operator.OPERATOR_EQUALS)
                                      .setValue(String.format("request-header-%s", i)))))
              .build());
    }

    for (int i = 0; i < 5; i++) {
      this.spanProcessingConfigServiceStub.createSpanIngestionRule(
          CreateSpanIngestionRuleRequest.newBuilder()
              .setStage(IngestionStage.INGESTION_STAGE_QUERY_STORE_PERSISTENCE)
              .setResponseHeaderRule(
                  KeyValueRetentionRuleData.newBuilder()
                      .setAction(
                          RetentionAction.newBuilder()
                              .setRetain(RetentionAction.RetainAction.getDefaultInstance()))
                      .setPredicate(
                          Predicate.newBuilder()
                              .setTargetKeyPredicate(
                                  StringPredicate.newBuilder()
                                      .setOperator(StringPredicate.Operator.OPERATOR_EQUALS)
                                      .setValue(String.format("response-header-%s", i)))))
              .build());
    }

    KeyValueRetentionRule requestHeaderRuleToChangeRank = this.getRequestHeaderRules().get(1);
    KeyValueRetentionRule responseHeaderRuleToChangeRank = this.getResponseHeaderRules().get(2);

    this.spanProcessingConfigServiceStub.rankSpanIngestionRule(
        RankSpanIngestionRuleRequest.newBuilder()
            .setIdToUpdate(requestHeaderRuleToChangeRank.getId())
            .setPrecedingRuleId(this.getRequestHeaderRules().get(4).getId())
            .build());

    this.spanProcessingConfigServiceStub.rankSpanIngestionRule(
        RankSpanIngestionRuleRequest.newBuilder()
            .setIdToUpdate(responseHeaderRuleToChangeRank.getId())
            .setPrecedingRuleId(this.getResponseHeaderRules().get(3).getId())
            .build());

    assertEquals(requestHeaderRuleToChangeRank, this.getRequestHeaderRules().get(4));
    assertEquals(responseHeaderRuleToChangeRank, this.getResponseHeaderRules().get(3));

    KeyValueRetentionRule firstRequestHeaderRule = this.getRequestHeaderRules().get(0);
    KeyValueRetentionRule updatedRule =
        this.spanProcessingConfigServiceStub
            .updateSpanIngestionRule(
                UpdateSpanIngestionRuleRequest.newBuilder()
                    .setId(firstRequestHeaderRule.getId())
                    .setKeyValueRetentionRuleData(
                        firstRequestHeaderRule.getData().toBuilder()
                            .setPredicate(
                                Predicate.newBuilder()
                                    .setTargetKeyPredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(
                                                StringPredicate.Operator.OPERATOR_MATCHES_REGEX)
                                            .setValue("request-header-updated")))
                            .setExpiration(Timestamp.newBuilder().setSeconds(1913807000)))
                    .build())
            .getRule();

    assertEquals(updatedRule, this.getRequestHeaderRules().get(0));
    this.spanProcessingConfigServiceStub.deleteSpanIngestionRule(
        DeleteSpanIngestionRuleRequest.newBuilder().setId(firstRequestHeaderRule.getId()).build());
    assertEquals(4, this.getRequestHeaderRules().size());
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  private List<KeyValueRetentionRule> getRequestHeaderRules() {
    return this.spanProcessingConfigServiceStub
        .getSpanIngestionConfig(VALID_GET_CONFIG_REQUEST)
        .getRequestHeaderRuleset()
        .getRulesList();
  }

  private List<KeyValueRetentionRule> getResponseHeaderRules() {
    return this.spanProcessingConfigServiceStub
        .getSpanIngestionConfig(VALID_GET_CONFIG_REQUEST)
        .getResponseHeaderRuleset()
        .getRulesList();
  }
}
