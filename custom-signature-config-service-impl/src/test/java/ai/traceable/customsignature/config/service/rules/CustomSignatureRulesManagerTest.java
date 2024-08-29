package ai.traceable.customsignature.config.service.rules;

import static ai.traceable.customsignature.config.service.rules.CustomSignatureRulesStore.CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE;
import static ai.traceable.customsignature.config.service.rules.CustomSignatureRulesStore.CUSTOM_SIGNATURE_RULE_CONFIG_RESOURCE_NAME;
import static ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_NOT_EQUAL;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import ai.traceable.customsignature.config.service.v1.AttributeKeyValueExpression;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.customsignature.config.service.v1.RuleSource;
import ai.traceable.customsignature.config.service.v1.StringCondition;
import com.google.common.collect.ImmutableSortedMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomSignatureRulesManagerTest {

  private MockGenericConfigService mockConfigService;
  private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private CustomSignatureRuleConverter ruleConverter;
  private CustomSignatureRulesManager rulesManager;
  private RequestContext requestContext;

  @BeforeEach
  void setup() {
    mockConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockUpsertAll();
    mockConfigService.start();
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    ruleConverter = spy(CustomSignatureRuleConverter.class);
    CustomSignatureRulesStore rulesStore =
        new CustomSignatureRulesStore(
            configServiceBlockingStub, ruleConverter, mock(ConfigChangeEventGenerator.class));
    this.rulesManager = spy(new CustomSignatureRulesManager(rulesStore));
    requestContext = RequestContext.forTenantId("default tenant");
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Test
  void testGetRules() {
    List<CustomSignatureRule> expectedRules =
        List.of(
            CustomSignatureRule.newBuilder()
                .setId("id1")
                .setName("name-1")
                .setDisabled(true)
                .setInternal(true)
                .setEffect(
                    RuleEffect.newBuilder()
                        .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING))
                .setRuleScope(getRuleScope(List.of("dev", "prod")))
                .setRuleSource(RuleSource.RULE_SOURCE_SYSTEM)
                .build(),
            CustomSignatureRule.newBuilder()
                .setId("id2")
                .setName("name-2")
                .setEffect(
                    RuleEffect.newBuilder().setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION))
                .setRuleScope(getRuleScope(List.of("dev")))
                .build(),
            CustomSignatureRule.newBuilder()
                .setId("id3")
                .setName("name-3")
                .setDefinition(
                    RuleDefinition.newBuilder().putLabels("label-key-3", "label-value-3"))
                .setEffect(
                    RuleEffect.newBuilder().setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION))
                .build(),
            CustomSignatureRule.newBuilder()
                .setId("id4")
                .setName("name-4")
                .setDefinition(
                    RuleDefinition.newBuilder()
                        .setClauseGroup(
                            ClauseGroup.newBuilder()
                                .addClauses(
                                    Clause.newBuilder()
                                        .setAttributeKeyValueExpression(
                                            AttributeKeyValueExpression.newBuilder()
                                                .setKeyMatchOperator(MATCH_OPERATOR_NOT_EQUAL)
                                                .setMatchKey("key")
                                                .setValueMatchOperator(MATCH_OPERATOR_NOT_EQUAL)
                                                .setMatchValue("value"))))
                        .putLabels("label-key-4", "label-value-4"))
                .setEffect(
                    RuleEffect.newBuilder().setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION))
                .build());

    when(rulesManager.generateRuleId())
        .thenReturn("id1")
        .thenReturn("id2")
        .thenReturn("id3")
        .thenReturn("id4");
    rulesManager.createCustomSignatureRule(
        requestContext,
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name-1")
            .setRuleSource(RuleSource.RULE_SOURCE_SYSTEM)
            .build());
    rulesManager.createCustomSignatureRule(
        requestContext, CreateCustomSignatureRuleRequest.newBuilder().setName("name-2").build());
    rulesManager.createCustomSignatureRule(
        requestContext, CreateCustomSignatureRuleRequest.newBuilder().setName("name-3").build());
    rulesManager.createCustomSignatureRule(
        requestContext, CreateCustomSignatureRuleRequest.newBuilder().setName("name-4").build());
    rulesManager.updateCustomSignatureRule(requestContext, expectedRules.get(0));
    rulesManager.updateCustomSignatureRule(requestContext, expectedRules.get(1));
    rulesManager.updateCustomSignatureRule(requestContext, expectedRules.get(2));
    rulesManager.updateCustomSignatureRule(requestContext, expectedRules.get(3));

    List<CustomSignatureRule> results;

    // Filter by id but id not present
    assertTrue(
        rulesManager
            .getCustomSignatureRules(
                requestContext, GetRulesFilter.newBuilder().addRuleIds("id").build())
            .isEmpty());

    // Filter by id and id is present
    results =
        rulesManager.getCustomSignatureRules(
            requestContext, GetRulesFilter.newBuilder().addRuleIds("id2").build());
    assertEquals(1, results.size());
    assertEquals(expectedRules.get(1), results.get(0));

    // No filter -- return all rules
    requestContext = RequestContext.forTenantId("tenant");
    results =
        rulesManager.getCustomSignatureRules(requestContext, GetRulesFilter.newBuilder().build());
    CustomSignatureRule expectedRule =
        CustomSignatureRule.newBuilder()
            .setId("id4")
            .setName("name-4")
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .addClauses(
                                Clause.newBuilder()
                                    .setAttributeKeyValueExpression(
                                        AttributeKeyValueExpression.newBuilder()
                                            .setKeyMatchOperator(MATCH_OPERATOR_NOT_EQUAL)
                                            .setMatchKey("key")
                                            .setValueMatchOperator(MATCH_OPERATOR_NOT_EQUAL)
                                            .setMatchValue("value")
                                            .setKeyCondition(
                                                StringCondition.newBuilder()
                                                    .setOperator(MATCH_OPERATOR_NOT_EQUAL)
                                                    .setValue("key"))
                                            .setValueCondition(
                                                StringCondition.newBuilder()
                                                    .setValue("value")
                                                    .setOperator(MATCH_OPERATOR_NOT_EQUAL)))))
                    .putLabels("label-key-4", "label-value-4"))
            .setEffect(RuleEffect.newBuilder().setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION))
            .build();
    assertEquals(4, results.size());
    assertTrue(results.contains(expectedRules.get(0)));
    assertTrue(results.contains(expectedRules.get(1)));
    assertTrue(results.contains(expectedRules.get(2)));
    assertTrue(results.contains(expectedRule));

    // Filter on disabled
    results =
        rulesManager.getCustomSignatureRules(
            requestContext, GetRulesFilter.newBuilder().setDisabled(true).build());
    assertEquals(1, results.size());
    assertEquals(expectedRules.get(0), results.get(0));

    // Filter on event type : present
    results =
        rulesManager.getCustomSignatureRules(
            requestContext,
            GetRulesFilter.newBuilder()
                .addEventTypes(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                .build());
    assertEquals(1, results.size());
    assertEquals(expectedRules.get(0), results.get(0));

    // Filter on event type : absent
    results =
        rulesManager.getCustomSignatureRules(
            requestContext,
            GetRulesFilter.newBuilder()
                .addEventTypes(EventType.EVENT_TYPE_TESTING_DETECTION)
                .build());
    assertTrue(results.isEmpty());

    // Filter on event type : present and internal : present
    results =
        rulesManager.getCustomSignatureRules(
            requestContext,
            GetRulesFilter.newBuilder()
                .addEventTypes(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                .setInternal(true)
                .build());
    assertEquals(1, results.size());
    assertEquals(expectedRules.get(0), results.get(0));

    // Filter on event type : present and internal : not present
    results =
        rulesManager.getCustomSignatureRules(
            requestContext,
            GetRulesFilter.newBuilder()
                .addEventTypes(EventType.EVENT_TYPE_NORMAL_DETECTION)
                .setInternal(true)
                .build());
    assertTrue(results.isEmpty());

    // Filter on dev env (all 3 rules should come)
    results =
        rulesManager.getCustomSignatureRules(
            requestContext,
            GetRulesFilter.newBuilder().setRuleScope(getRuleScope(List.of("dev"))).build());
    assertEquals(4, results.size());
    assertTrue(results.contains(expectedRules.get(0)));
    assertTrue(results.contains(expectedRules.get(1)));
    assertTrue(results.contains(expectedRules.get(2)));

    // Filter on prod env (only 2 rules should come)
    results =
        rulesManager.getCustomSignatureRules(
            requestContext,
            GetRulesFilter.newBuilder().setRuleScope(getRuleScope(List.of("prod"))).build());
    assertEquals(3, results.size());
    assertTrue(results.contains(expectedRules.get(0)));
    assertTrue(results.contains(expectedRules.get(2)));

    // Filter on staging env (first 2 rules should get filtered out)
    results =
        rulesManager.getCustomSignatureRules(
            requestContext,
            GetRulesFilter.newBuilder().setRuleScope(getRuleScope(List.of("staging"))).build());
    assertEquals(2, results.size());
    assertTrue(results.contains(expectedRules.get(2)));

    // Filter by rule scope with env scope with no envs should only return rules with no envs
    results =
        rulesManager.getCustomSignatureRules(
            requestContext,
            GetRulesFilter.newBuilder().setRuleScope(getRuleScope(List.of())).build());
    assertEquals(2, results.size());
    assertTrue(results.contains(expectedRules.get(2)));

    // Filter by rule scope with no env scope should all available rules
    results =
        rulesManager.getCustomSignatureRules(
            requestContext,
            GetRulesFilter.newBuilder().setRuleScope(RuleScope.getDefaultInstance()).build());
    assertEquals(4, results.size());
    assertTrue(results.contains(expectedRules.get(0)));
    assertTrue(results.contains(expectedRules.get(1)));
    assertTrue(results.contains(expectedRules.get(2)));

    // Filter on rule source
    results =
        rulesManager.getCustomSignatureRules(
            requestContext,
            GetRulesFilter.newBuilder().addRuleSources(RuleSource.RULE_SOURCE_SYSTEM).build());
    assertEquals(1, results.size());
    assertEquals(expectedRules.get(0), results.get(0));

    // Filter on labels
    results =
        rulesManager.getCustomSignatureRules(
            requestContext, GetRulesFilter.newBuilder().addLabelKeys("label-key-3").build());
    assertEquals(1, results.size());
    assertEquals(expectedRules.get(2), results.get(0));
  }

  @Test
  void testCreateRule() throws InvalidProtocolBufferException {
    when(rulesManager.generateRuleId()).thenReturn("id");

    CustomSignatureRule customSignatureRule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setDefinition(RuleDefinition.newBuilder().build())
            .setEffect(RuleEffect.newBuilder().build())
            .setRuleScope(getRuleScope(List.of("dev")))
            .setInternal(true)
            .build();
    CreateCustomSignatureRuleRequest createRuleRequest =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setRuleScope(getRuleScope(List.of("dev")))
            .setInternal(true)
            .build();
    assertEquals(
        customSignatureRule,
        rulesManager.createCustomSignatureRule(requestContext, createRuleRequest).get());

    when(ruleConverter.convert(customSignatureRule))
        .thenThrow(new InvalidProtocolBufferException("invalid"))
        .thenCallRealMethod();
    assertTrue(rulesManager.createCustomSignatureRule(requestContext, createRuleRequest).isEmpty());

    when(ruleConverter.convert((Value) any()))
        .thenThrow(new InvalidProtocolBufferException("invalid"))
        .thenCallRealMethod();
    assertTrue(rulesManager.createCustomSignatureRule(requestContext, createRuleRequest).isEmpty());
  }

  @Test
  void testUpdateRule() throws InvalidProtocolBufferException {
    CustomSignatureRule customSignatureRule =
        CustomSignatureRule.newBuilder().setId("id").setName("name").build();
    assertTrue(
        rulesManager.updateCustomSignatureRule(requestContext, customSignatureRule).isEmpty());

    Value mockRuleConfig = mockRuleConfig("id", "name-1");
    upsertRuleConfigs(ImmutableSortedMap.of("id", mockRuleConfig));
    assertEquals(
        customSignatureRule,
        rulesManager.updateCustomSignatureRule(requestContext, customSignatureRule).get());

    when(ruleConverter.convert(customSignatureRule))
        .thenThrow(new InvalidProtocolBufferException("invalid"))
        .thenCallRealMethod();
    assertTrue(
        rulesManager.updateCustomSignatureRule(requestContext, customSignatureRule).isEmpty());

    when(ruleConverter.convert((Value) any()))
        .thenThrow(new InvalidProtocolBufferException("invalid"))
        .thenCallRealMethod();
    assertTrue(
        rulesManager.updateCustomSignatureRule(requestContext, customSignatureRule).isEmpty());
  }

  @Test
  void testDeleteRule() {
    String id = "id-1";
    Value mockRegionRuleConfig = mockRuleConfig(id, "name-1");

    upsertRuleConfigs(ImmutableSortedMap.of(id, mockRegionRuleConfig));
    assertFalse(
        rulesManager
            .getCustomSignatureRules(
                requestContext, GetRulesFilter.newBuilder().addRuleIds(id).build())
            .isEmpty());
    assertFalse(
        rulesManager
            .getCustomSignatureRules(
                requestContext, GetRulesFilter.newBuilder().addRuleIds(id).build())
            .isEmpty());
    assertDoesNotThrow(() -> rulesManager.deleteCustomSignatureRule(requestContext, id));
    assertTrue(
        rulesManager
            .getCustomSignatureRules(
                requestContext, GetRulesFilter.newBuilder().addRuleIds(id).build())
            .isEmpty());
  }

  @Test
  void testBlockingExpiryForCreateRule() {
    when(rulesManager.generateRuleId()).thenReturn("id");

    // Expiry duration and expiry timestamp are not set
    CustomSignatureRule customSignatureRule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setDefinition(RuleDefinition.newBuilder().build())
            .setEffect(RuleEffect.newBuilder().build())
            .setRuleScope(RuleScope.newBuilder())
            .build();
    CreateCustomSignatureRuleRequest createRuleRequest =
        CreateCustomSignatureRuleRequest.newBuilder().setName("name").build();
    assertEquals(
        customSignatureRule,
        rulesManager.createCustomSignatureRule(requestContext, createRuleRequest).get());

    // Expiry duration is set, expiry timestamp not set
    createRuleRequest =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setBlockingExpiryDetails(ExpiryDetails.newBuilder().setExpiryDuration("PT2H").build())
            .build();
    long expiryTimestampMillis =
        rulesManager
            .createCustomSignatureRule(requestContext, createRuleRequest)
            .get()
            .getBlockingExpiryDetails()
            .getExpiryTimestampMillis();
    assertTrue(expiryTimestampMillis > System.currentTimeMillis() + 1 * 60 * 60 * 1000);

    // Expiry duration is not set, expiry timestamp is set
    createRuleRequest =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder()
                    .setExpiryTimestampMillis(System.currentTimeMillis() + 5 * 24 * 3600 * 1000)
                    .build())
            .build();
    String expiryDuration =
        rulesManager
            .createCustomSignatureRule(requestContext, createRuleRequest)
            .get()
            .getBlockingExpiryDetails()
            .getExpiryDuration();
    assertTrue(Duration.parse(expiryDuration).toDays() >= 4);

    // Expiry duration and expiry timestamp are set
    createRuleRequest =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder()
                    .setExpiryDuration("PT2H")
                    .setExpiryTimestampMillis(23456789)
                    .build())
            .build();
    expiryTimestampMillis =
        rulesManager
            .createCustomSignatureRule(requestContext, createRuleRequest)
            .get()
            .getBlockingExpiryDetails()
            .getExpiryTimestampMillis();
    assertEquals(23456789, expiryTimestampMillis);
  }

  @Test
  void testBlockingExpiryForUpdateRule() {
    CustomSignatureRule customSignatureRule =
        CustomSignatureRule.newBuilder().setId("id").setName("name").build();
    assertTrue(
        rulesManager.updateCustomSignatureRule(requestContext, customSignatureRule).isEmpty());

    Value mockRuleConfig = mockRuleConfig("id", "name-1");
    upsertRuleConfigs(ImmutableSortedMap.of("id", mockRuleConfig));

    // Expiry duration is set, expiry timestamp not set
    customSignatureRule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setBlockingExpiryDetails(ExpiryDetails.newBuilder().setExpiryDuration("PT2H").build())
            .build();
    long expiryTimestampMillis =
        rulesManager
            .updateCustomSignatureRule(requestContext, customSignatureRule)
            .get()
            .getBlockingExpiryDetails()
            .getExpiryTimestampMillis();
    assertTrue(expiryTimestampMillis > System.currentTimeMillis() + 1 * 3600 * 1000);

    // Expiry duration is not set, expiry timestamp is set
    customSignatureRule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder()
                    .setExpiryTimestampMillis(System.currentTimeMillis() + 5 * 24 * 3600 * 1000)
                    .build())
            .build();
    String expiryDuration =
        rulesManager
            .updateCustomSignatureRule(requestContext, customSignatureRule)
            .get()
            .getBlockingExpiryDetails()
            .getExpiryDuration();
    assertTrue(Duration.parse(expiryDuration).toDays() >= 4);

    // Expiry duration and expiry timestamp are set
    customSignatureRule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder()
                    .setExpiryDuration("PT2H")
                    .setExpiryTimestampMillis(23456789)
                    .build())
            .build();
    expiryTimestampMillis =
        rulesManager
            .updateCustomSignatureRule(requestContext, customSignatureRule)
            .get()
            .getBlockingExpiryDetails()
            .getExpiryTimestampMillis();
    assertEquals(23456789, expiryTimestampMillis);

    // Expiry duration and expiry timestamp are not set
    customSignatureRule = CustomSignatureRule.newBuilder().setId("id").setName("name").build();
    assertEquals(
        customSignatureRule,
        rulesManager.updateCustomSignatureRule(requestContext, customSignatureRule).get());
  }

  private void upsertRuleConfigs(Map<String, Value> ruleConfigs) {
    ruleConfigs.forEach(
        (id, ruleConfig) ->
            configServiceBlockingStub.upsertConfig(
                UpsertConfigRequest.newBuilder()
                    .setResourceNamespace(CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE)
                    .setResourceName(CUSTOM_SIGNATURE_RULE_CONFIG_RESOURCE_NAME)
                    .setConfig(ruleConfig)
                    .setContext(id)
                    .build()));
  }

  private Value mockRuleConfig(String id, String name) {
    Struct ruleConfigStruct =
        Struct.newBuilder()
            .putFields("id", Value.newBuilder().setStringValue(id).build())
            .putFields("name", Value.newBuilder().setStringValue(name).build())
            .build();
    return Value.newBuilder().setStructValue(ruleConfigStruct).build();
  }

  private RuleScope getRuleScope(List<String> environmentIds) {
    return RuleScope.newBuilder()
        .setEnvironmentScope(EnvironmentScope.newBuilder().addAllEnvironmentIds(environmentIds))
        .build();
  }
}
