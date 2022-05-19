package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.CORE_MODE_RULE_CATEGORY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceImplBase;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetsResponse;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import ai.traceable.sensitivedata.config.service.v1.Condition;
import ai.traceable.sensitivedata.config.service.v1.Condition.AttributeRegexMatch;
import ai.traceable.sensitivedata.config.service.v1.CreateRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.DeleteRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.DropUnparsedJsonPolicy;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest.RedactionRuleFilter;
import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.GetFullPrivacyModeRequest;
import ai.traceable.sensitivedata.config.service.v1.GetInvalidJsonPolicyRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.InvalidJsonPolicy;
import ai.traceable.sensitivedata.config.service.v1.MatchType;
import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.UpdateAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateFullPrivacyModeRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateFullPrivacyModeResponse;
import ai.traceable.sensitivedata.config.service.v1.UpdateInvalidJsonPolicyRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateInvalidJsonPolicyResponse;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyForTypeRequest;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SensitiveDataConfigServiceImplTest {
  SensitiveDataConfigServiceBlockingStub sensitiveDataStub;
  MockGenericConfigService mockGenericConfigService;
  SensitiveDataServiceConfig mockConfig;

  DefaultRedactionRulePersistenceStatusStore mockPersistenceStatusStore;

  @BeforeEach
  void beforeEach() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();

    this.mockConfig = mock(SensitiveDataServiceConfig.class);
    this.mockPersistenceStatusStore = mock(DefaultRedactionRulePersistenceStatusStore.class);
    DefaultRedactionRules mockDefaultRedactionRules = mock(DefaultRedactionRules.class);
    when(mockConfig.defaultAutomaticRedactionStrategy()).thenReturn(true);
    when(mockConfig.defaultParamTypeRedactionStrategy())
        .thenReturn(RedactionStrategy.REDACTION_STRATEGY_RAW);
    when(mockConfig.defaultRedactionRules()).thenReturn(mockDefaultRedactionRules);
    when(mockConfig.defaultInvalidJsonPolicy())
        .thenReturn(
            InvalidJsonPolicy.newBuilder()
                .setDropUnparsedJsonPolicy(DropUnparsedJsonPolicy.getDefaultInstance())
                .build());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
    mockGenericConfigService
        .addService(
            new SensitiveDataConfigServiceImpl(
                new ConfigServiceCoordinatorImpl(
                    configServiceBlockingStub,
                    configChangeEventGenerator,
                    mockConfig,
                    new RedactionRuleConfigStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    new AutomaticSecretRedactionStrategyConfigStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    new InvalidJsonPolicyConfigStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    new FullPrivacyModeConfigStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    mockPersistenceStatusStore,
                    DataClassificationConfigServiceGrpc.newBlockingStub(
                        mockGenericConfigService.channel()))))
        .addService(new MockDataClassificationConfigService())
        .start();

    sensitiveDataStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void upsertAndGetRedactionStrategyForType() {
    RedactionStrategy redactionStrategy =
        sensitiveDataStub
            .getRedactionStrategyForType(
                GetRedactionStrategyForTypeRequest.newBuilder()
                    .setParamType(ParamType.PARAM_TYPE_HEADER)
                    .build())
            .getRedactionStrategy();
    assertEquals(RedactionStrategy.REDACTION_STRATEGY_RAW, redactionStrategy);

    sensitiveDataStub.updateRedactionStrategyForType(
        UpdateRedactionStrategyForTypeRequest.newBuilder()
            .setParamType(ParamType.PARAM_TYPE_HEADER)
            .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_REDACT)
            .build());
    redactionStrategy =
        sensitiveDataStub
            .getRedactionStrategyForType(
                GetRedactionStrategyForTypeRequest.newBuilder()
                    .setParamType(ParamType.PARAM_TYPE_HEADER)
                    .build())
            .getRedactionStrategy();
    assertEquals(RedactionStrategy.REDACTION_STRATEGY_REDACT, redactionStrategy);
  }

  @Test
  void upsertAndGetAutomaticSecretRedactionStrategy() {
    boolean automaticSecretRedactionEnabled =
        sensitiveDataStub
            .getAutomaticSecretRedactionStrategy(
                GetAutomaticSecretRedactionStrategyRequest.newBuilder().build())
            .getEnabled();
    assertEquals(true, automaticSecretRedactionEnabled);

    sensitiveDataStub.updateAutomaticSecretRedactionStrategy(
        UpdateAutomaticSecretRedactionStrategyRequest.newBuilder().setEnabled(false).build());
    automaticSecretRedactionEnabled =
        sensitiveDataStub
            .getAutomaticSecretRedactionStrategy(
                GetAutomaticSecretRedactionStrategyRequest.newBuilder().build())
            .getEnabled();
    assertEquals(false, automaticSecretRedactionEnabled);
  }

  @Test
  void createReadUpdateDeleteRedactionRules() {
    RedactionRule defaultRedactionRule =
        RedactionRule.newBuilder()
            .setId("default-rule-id")
            .setName("name")
            .setDescription("default rule")
            .setCategory("core_mode")
            .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_REDACT)
            .setMatchType(MatchType.MATCH_TYPE_KEY)
            .setRegex("regex")
            .build();
    when(mockPersistenceStatusStore.getData(any())).thenReturn(Optional.empty());
    when(mockConfig
            .defaultRedactionRules()
            .getUnpersistedRules(DefaultRedactionRulePersistenceStatus.empty()))
        .thenReturn(List.of(defaultRedactionRule));
    NewRedactionRule newRedactionRule1 = getNewRedactionRule("rule1", "^password");
    NewRedactionRule newRedactionRule2 = getNewRedactionRule("rule2", "^name");
    RedactionRule redactionRule1 =
        sensitiveDataStub
            .createRedactionRule(
                CreateRedactionRuleRequest.newBuilder()
                    .setNewRedactionRule(newRedactionRule1)
                    .build())
            .getRedactionRule();
    assertEquals(getRedactionRule(newRedactionRule1, redactionRule1.getId()), redactionRule1);

    RedactionRule redactionRule2 =
        sensitiveDataStub
            .createRedactionRule(
                CreateRedactionRuleRequest.newBuilder()
                    .setNewRedactionRule(newRedactionRule2)
                    .build())
            .getRedactionRule();

    assertEquals(
        List.of(defaultRedactionRule, redactionRule1, redactionRule2),
        sensitiveDataStub
            .getAllRedactionRules(GetAllRedactionRulesRequest.getDefaultInstance())
            .getRedactionRulesList());

    RedactionRule ruleToUpdate =
        redactionRule1.toBuilder()
            .setName("rule1a")
            .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_HASH)
            .build();
    RedactionRule updatedRule =
        sensitiveDataStub
            .updateRedactionRule(
                UpdateRedactionRuleRequest.newBuilder().setRedactionRule(ruleToUpdate).build())
            .getRedactionRule();
    assertEquals(ruleToUpdate, updatedRule);

    assertEquals(
        List.of(defaultRedactionRule, updatedRule, redactionRule2),
        sensitiveDataStub
            .getAllRedactionRules(GetAllRedactionRulesRequest.newBuilder().build())
            .getRedactionRulesList());

    sensitiveDataStub.deleteRedactionRule(
        DeleteRedactionRuleRequest.newBuilder().setRedactionRuleId(redactionRule2.getId()).build());
    assertEquals(
        List.of(defaultRedactionRule, updatedRule),
        sensitiveDataStub
            .getAllRedactionRules(GetAllRedactionRulesRequest.newBuilder().build())
            .getRedactionRulesList());
  }

  @Test
  void updateAndDeleteDefaultRedactionRules() {
    RedactionRule defaultRedactionRule1 =
        RedactionRule.newBuilder().setId("default-rule-id-1").setName("rule 1").build();

    RedactionRule defaultRedactionRule2 =
        RedactionRule.newBuilder().setId("default-rule-id-2").setName("rule 2").build();

    when(mockPersistenceStatusStore.getData(any())).thenReturn(Optional.empty());
    when(mockConfig
            .defaultRedactionRules()
            .getUnpersistedRules(DefaultRedactionRulePersistenceStatus.empty()))
        .thenReturn(List.of(defaultRedactionRule1, defaultRedactionRule2));

    when(mockConfig.defaultRedactionRules().isDefaultRuleId(defaultRedactionRule1.getId()))
        .thenReturn(true);
    when(mockConfig.defaultRedactionRules().isDefaultRuleId(defaultRedactionRule2.getId()))
        .thenReturn(true);
    assertEquals(
        List.of(defaultRedactionRule1, defaultRedactionRule2),
        sensitiveDataStub
            .getAllRedactionRules(GetAllRedactionRulesRequest.getDefaultInstance())
            .getRedactionRulesList());

    RedactionRule ruleToUpdate =
        defaultRedactionRule1.toBuilder().setName("updated default rule 1").build();
    RedactionRule updatedRule =
        sensitiveDataStub
            .updateRedactionRule(
                UpdateRedactionRuleRequest.newBuilder().setRedactionRule(ruleToUpdate).build())
            .getRedactionRule();

    // Should be updated to have a real uuid
    assertNotEquals(defaultRedactionRule1.getId(), updatedRule.getId());
    assertDoesNotThrow(() -> UUID.fromString(updatedRule.getId()));
    assertEquals(ruleToUpdate.toBuilder().setId(updatedRule.getId()).build(), updatedRule);

    // Now make sure we upserted the updated id, and if so, mock returning that
    DefaultRedactionRulePersistenceStatus currentPersistenceStatus =
        DefaultRedactionRulePersistenceStatus.of(List.of(defaultRedactionRule1.getId()));
    verify(mockPersistenceStatusStore, times(1)).upsertObject(any(), eq(currentPersistenceStatus));
    when(mockPersistenceStatusStore.getData(any()))
        .thenReturn(Optional.of(currentPersistenceStatus));
    when(mockConfig.defaultRedactionRules().getUnpersistedRules(currentPersistenceStatus))
        .thenReturn(List.of(defaultRedactionRule2));

    assertEquals(
        List.of(defaultRedactionRule2, updatedRule),
        sensitiveDataStub
            .getAllRedactionRules(GetAllRedactionRulesRequest.newBuilder().build())
            .getRedactionRulesList());

    when(mockConfig.defaultRedactionRules().getRule(defaultRedactionRule2.getId()))
        .thenReturn(Optional.of(defaultRedactionRule2));
    // Now delete the first default rule
    sensitiveDataStub.deleteRedactionRule(
        DeleteRedactionRuleRequest.newBuilder()
            .setRedactionRuleId(defaultRedactionRule2.getId())
            .build());

    // Now make sure we upserted the delete id, and if so, mock returning that
    currentPersistenceStatus =
        DefaultRedactionRulePersistenceStatus.of(
            List.of(defaultRedactionRule1.getId(), defaultRedactionRule2.getId()));
    verify(mockPersistenceStatusStore, times(1)).upsertObject(any(), eq(currentPersistenceStatus));
    when(mockPersistenceStatusStore.getData(any()))
        .thenReturn(Optional.of(currentPersistenceStatus));
    when(mockConfig.defaultRedactionRules().getUnpersistedRules(currentPersistenceStatus))
        .thenReturn(List.of());

    assertEquals(
        List.of(updatedRule),
        sensitiveDataStub
            .getAllRedactionRules(GetAllRedactionRulesRequest.newBuilder().build())
            .getRedactionRulesList());
  }

  @Test
  void createRedactionRuleWithInvalidRegexShouldFail() {
    NewRedactionRule newRedactionRule = getNewRedactionRule("rule1", "pass**");
    assertThrows(
        StatusRuntimeException.class,
        () ->
            sensitiveDataStub
                .createRedactionRule(
                    CreateRedactionRuleRequest.newBuilder()
                        .setNewRedactionRule(newRedactionRule)
                        .build())
                .getRedactionRule());
  }

  @Test
  void upsertAndGetFullPrivacyMode() {
    // Should be false when not set
    boolean fullPrivacyMode =
        sensitiveDataStub
            .getFullPrivacyMode(GetFullPrivacyModeRequest.newBuilder().build())
            .getEnabled();
    assertFalse(fullPrivacyMode);

    // Set to true
    UpdateFullPrivacyModeResponse updateFullPrivacyModeResponse =
        sensitiveDataStub.updateFullPrivacyMode(
            UpdateFullPrivacyModeRequest.newBuilder().setEnabled(true).build());
    assertNotNull(updateFullPrivacyModeResponse);
    fullPrivacyMode =
        sensitiveDataStub
            .getFullPrivacyMode(GetFullPrivacyModeRequest.newBuilder().build())
            .getEnabled();
    assertTrue(fullPrivacyMode);

    // Set to false
    updateFullPrivacyModeResponse =
        sensitiveDataStub.updateFullPrivacyMode(
            UpdateFullPrivacyModeRequest.newBuilder().setEnabled(false).build());
    assertNotNull(updateFullPrivacyModeResponse);
    fullPrivacyMode =
        sensitiveDataStub
            .getFullPrivacyMode(GetFullPrivacyModeRequest.newBuilder().build())
            .getEnabled();
    assertFalse(fullPrivacyMode);
  }

  @Test
  void upsertAndGetInvalidJsonPolicyConfig() {
    InvalidJsonPolicy invalidJsonPolicy =
        sensitiveDataStub
            .getInvalidJsonPolicy(GetInvalidJsonPolicyRequest.newBuilder().build())
            .getInvalidJsonPolicy();
    assertTrue(invalidJsonPolicy.hasDropUnparsedJsonPolicy());

    // Set to unspecified
    UpdateInvalidJsonPolicyResponse updateInvalidJsonPolicyResponse =
        sensitiveDataStub.updateInvalidJsonPolicy(
            UpdateInvalidJsonPolicyRequest.newBuilder()
                .setInvalidJsonPolicy(InvalidJsonPolicy.getDefaultInstance())
                .build());
    assertNotNull(updateInvalidJsonPolicyResponse);
    invalidJsonPolicy =
        sensitiveDataStub
            .getInvalidJsonPolicy(GetInvalidJsonPolicyRequest.newBuilder().build())
            .getInvalidJsonPolicy();
    assertFalse(invalidJsonPolicy.hasDropUnparsedJsonPolicy());
  }

  @Test
  void conditionalRuleFiltering() {
    RedactionRule unconditionalRule =
        sensitiveDataStub
            .createRedactionRule(
                CreateRedactionRuleRequest.newBuilder()
                    .setNewRedactionRule(getNewRedactionRule("conditional-rule", "^password"))
                    .build())
            .getRedactionRule();

    RedactionRule conditionalRule =
        sensitiveDataStub
            .createRedactionRule(
                CreateRedactionRuleRequest.newBuilder()
                    .setNewRedactionRule(
                        getNewRedactionRule("unconditional-rule", "^password").toBuilder()
                            .addConditions(
                                Condition.newBuilder()
                                    .setAttributeRegexMatch(
                                        AttributeRegexMatch.newBuilder()
                                            .setKey("foo")
                                            .setRegex("bar"))))
                    .build())
            .getRedactionRule();

    assertEquals(
        List.of(unconditionalRule, conditionalRule),
        sensitiveDataStub
            .getAllRedactionRules(GetAllRedactionRulesRequest.getDefaultInstance())
            .getRedactionRulesList());

    assertEquals(
        List.of(conditionalRule),
        sensitiveDataStub
            .getAllRedactionRules(
                GetAllRedactionRulesRequest.newBuilder()
                    .setFilter(RedactionRuleFilter.newBuilder().setIsConditional(true))
                    .build())
            .getRedactionRulesList());

    assertEquals(
        List.of(unconditionalRule),
        sensitiveDataStub
            .getAllRedactionRules(
                GetAllRedactionRulesRequest.newBuilder()
                    .setFilter(RedactionRuleFilter.newBuilder().setIsConditional(false))
                    .build())
            .getRedactionRulesList());
  }

  @Test
  void sensitiveRuleFiltering() {
    RedactionRule sensitiveRule =
        sensitiveDataStub
            .createRedactionRule(
                CreateRedactionRuleRequest.newBuilder()
                    .setNewRedactionRule(getNewRedactionRule("sensitive-rule", "^password"))
                    .build())
            .getRedactionRule();

    RedactionRule insensitiveRule =
        sensitiveDataStub
            .createRedactionRule(
                CreateRedactionRuleRequest.newBuilder()
                    .setNewRedactionRule(
                        getNewRedactionRule("insensitive-rule", "^password").toBuilder()
                            .setCategory(CORE_MODE_RULE_CATEGORY))
                    .build())
            .getRedactionRule();

    assertEquals(
        List.of(sensitiveRule, insensitiveRule),
        sensitiveDataStub
            .getAllRedactionRules(GetAllRedactionRulesRequest.getDefaultInstance())
            .getRedactionRulesList());

    assertEquals(
        List.of(sensitiveRule),
        sensitiveDataStub
            .getAllRedactionRules(
                GetAllRedactionRulesRequest.newBuilder()
                    .setFilter(RedactionRuleFilter.newBuilder().setIsSensitive(true))
                    .build())
            .getRedactionRulesList());

    assertEquals(
        List.of(insensitiveRule),
        sensitiveDataStub
            .getAllRedactionRules(
                GetAllRedactionRulesRequest.newBuilder()
                    .setFilter(RedactionRuleFilter.newBuilder().setIsSensitive(false))
                    .build())
            .getRedactionRulesList());
  }

  private NewRedactionRule getNewRedactionRule(String name, String regex) {
    return NewRedactionRule.newBuilder()
        .setName(name)
        .setDescription("sample rule")
        .setCategory("auth")
        .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_REDACT)
        .setMatchType(MatchType.MATCH_TYPE_KEY)
        .setRegex(regex)
        .build();
  }

  private RedactionRule getRedactionRule(NewRedactionRule newRedactionRule, String id) {
    RedactionRule.Builder builder =
        RedactionRule.newBuilder()
            .setId(id)
            .setName(newRedactionRule.getName())
            .setDescription(newRedactionRule.getDescription())
            .setCategory(newRedactionRule.getCategory())
            .setRedactionStrategy(newRedactionRule.getRedactionStrategy())
            .setMatchType(newRedactionRule.getMatchType())
            .setRegex(newRedactionRule.getRegex());
    if (newRedactionRule.hasComplexData()) {
      builder.setComplexData(newRedactionRule.getComplexData());
    }
    return builder.build();
  }

  private void assertRedactionRulesMatch(
      List<NewRedactionRule> expectedRules, List<RedactionRule> actualRules) {
    // Just using this method to ignore any generated ID
    if (expectedRules.size() != actualRules.size()) {
      fail("expected size should match actual size");
    }

    for (int index = 0; index < expectedRules.size(); index++) {
      RedactionRule expected = this.getRedactionRule(expectedRules.get(index), "generated-id");
      RedactionRule actual = actualRules.get(index).toBuilder().setId("generated-id").build();
      assertEquals(expected, actual);
    }
  }

  class MockDataClassificationConfigService extends DataClassificationConfigServiceImplBase {

    @Override
    public void getDataSets(
        GetDataSetsRequest request, StreamObserver<GetDataSetsResponse> responseObserver) {
      responseObserver.onNext(GetDataSetsResponse.newBuilder().build());
      responseObserver.onCompleted();
    }

    @Override
    public void getDataTypes(
        GetDataTypesRequest request, StreamObserver<GetDataTypesResponse> responseObserver) {
      responseObserver.onNext(GetDataTypesResponse.newBuilder().build());
      responseObserver.onCompleted();
    }
  }
}
