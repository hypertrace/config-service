package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.traceable.customsignature.config.service.v1.AgentModification;
import ai.traceable.customsignature.config.service.v1.AgentRuleEffect;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.EventSeverity;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.FieldValue;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEdgeDecisionRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEdgeDecisionRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.HeaderInjection;
import ai.traceable.customsignature.config.service.v1.IpAddressExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleEffectWithModifications;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.customsignature.config.service.v1.RuleSource;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CustomSignatureConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static final int DEFAULT_RULES_COUNT = 2;
  private static CustomSignatureConfigServiceBlockingStub configServiceStub;

  @BeforeAll
  static void init() {
    configServiceStub =
        CustomSignatureConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  public void testCreateRule() {
    assertEquals(DEFAULT_RULES_COUNT, fetchAllRules().size());
    List<CustomSignatureRule> createdRules = createRules();
    List<CustomSignatureRule> defaultRules =
        getDefaultRules(createdRules.get(0).getId(), createdRules.get(1).getId());
    assertEquals(defaultRules.get(0), createdRules.get(0));
    assertEquals(defaultRules.get(1), createdRules.get(1));
    assertEquals(2, fetchAllCreatedRules().size());
  }

  @Test()
  public void testUpdateRule() {
    assertEquals(DEFAULT_RULES_COUNT, fetchAllRules().size());
    List<CustomSignatureRule> createdRules = createRules();
    List<CustomSignatureRule> fetchedRules = fetchAllCreatedRules();
    assertEquals(2, fetchedRules.size());
    assertFalse(fetchedRules.get(0).getDisabled());
    assertFalse(fetchedRules.get(1).getDisabled());

    assertEquals(
        CustomSignatureRule.newBuilder(createdRules.get(0)).setDisabled(true).build(),
        disableRule(createdRules.get(0)));

    fetchedRules = fetchAllCreatedRules();
    assertEquals(2, fetchedRules.size());
    if (fetchedRules.get(0).getId().equals(createdRules.get(0).getId())) {
      assertTrue(fetchedRules.get(0).getDisabled());
      assertFalse(fetchedRules.get(1).getDisabled());
    } else {
      assertTrue(fetchedRules.get(1).getDisabled());
      assertFalse(fetchedRules.get(0).getDisabled());
    }
  }

  @Test()
  public void testDeleteRule() {
    assertEquals(DEFAULT_RULES_COUNT, fetchAllRules().size());
    List<CustomSignatureRule> createdRules = createRules();
    assertEquals(2, fetchAllCreatedRules().size());
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            configServiceStub.deleteCustomSignatureRule(
                DeleteCustomSignatureRuleRequest.newBuilder()
                    .setId(createdRules.get(1).getId())
                    .build()));
    List<CustomSignatureRule> fetchedRules = fetchAllCreatedRules();
    assertEquals(1, fetchedRules.size());
    assertEquals(createdRules.get(0).getId(), fetchedRules.get(0).getId());
  }

  @Test
  public void testGetRules() {
    assertEquals(DEFAULT_RULES_COUNT, fetchAllRules().size());
    List<CustomSignatureRule> createdRules = createRules();
    assertEquals(2, fetchAllCreatedRules().size());

    List<CustomSignatureRule> fetchedRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                configServiceStub
                    .getCustomSignatureRules(
                        GetCustomSignatureRulesRequest.newBuilder()
                            .setFilter(
                                GetRulesFilter.newBuilder()
                                    .addEventTypes(EventType.EVENT_TYPE_NORMAL_DETECTION)
                                    .addRuleSources(RuleSource.RULE_SOURCE_CUSTOMER)
                                    .build())
                            .build())
                    .getRulesList());
    assertEquals(1, fetchedRules.size());
    assertEquals(
        EventType.EVENT_TYPE_NORMAL_DETECTION, fetchedRules.get(0).getEffect().getEventType());
    assertEquals(createdRules.get(1).getId(), fetchedRules.get(0).getId());

    fetchedRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                configServiceStub
                    .getCustomSignatureRules(
                        GetCustomSignatureRulesRequest.newBuilder()
                            .setFilter(
                                GetRulesFilter.newBuilder()
                                    .setDisabled(true)
                                    .addRuleSources(RuleSource.RULE_SOURCE_CUSTOMER)
                                    .build())
                            .build())
                    .getRulesList());
    assertTrue(fetchedRules.isEmpty());

    disableRule(createdRules.get(0));
    fetchedRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                configServiceStub
                    .getCustomSignatureRules(
                        GetCustomSignatureRulesRequest.newBuilder()
                            .setFilter(
                                GetRulesFilter.newBuilder()
                                    .setDisabled(true)
                                    .addRuleSources(RuleSource.RULE_SOURCE_CUSTOMER)
                                    .build())
                            .build())
                    .getRulesList());
    assertEquals(1, fetchedRules.size());
    assertTrue(fetchedRules.get(0).getDisabled());
    assertEquals(createdRules.get(0).getId(), fetchedRules.get(0).getId());

    fetchedRules = fetchTestRules();
    assertFalse(fetchedRules.get(0).getBlockingExpiryDetails().hasExpiryDuration());
    assertEquals(0, fetchedRules.get(0).getBlockingExpiryDetails().getExpiryTimestampMillis());
    updateExpiryTime(fetchedRules.get(0));
    List<CustomSignatureRule> updatedRules = fetchTestRules(EventType.EVENT_TYPE_ALLOW);
    assertEquals(1, updatedRules.size());
    assertEquals(updatedRules.get(0).getId(), fetchedRules.get(0).getId());
    assertEquals(
        2,
        Duration.parse(updatedRules.get(0).getBlockingExpiryDetails().getExpiryDuration())
            .toDays());
    assertTrue(
        updatedRules.get(0).getBlockingExpiryDetails().getExpiryTimestampMillis()
            > System.currentTimeMillis());
  }

  @Test
  public void testGetModsecRules() {
    String modsecDirectives = "";
    URL directiveUrl = Resources.getResource("waf/waf-directives-test.conf");
    try {
      modsecDirectives = Resources.toString(directiveUrl, StandardCharsets.UTF_8) + "\n\n";
    } catch (IOException e) {
      fail("Failed to read waf directives file");
    }

    assertTrue(fetchAllCreatedRules().isEmpty());
    List<CustomSignatureRule> createdRules = createRules();
    assertEquals(2, fetchAllCreatedRules().size());

    GetCustomSignatureModsecRulesResponse rulesResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                configServiceStub.getCustomSignatureModsecRules(
                    GetCustomSignatureModsecRulesRequest.newBuilder()
                        .setFilter(
                            GetRulesFilter.newBuilder()
                                .addEventTypes(EventType.EVENT_TYPE_NORMAL_DETECTION)
                                .addRuleSources(RuleSource.RULE_SOURCE_CUSTOMER)
                                .build())
                        .build()));

    assertEquals(1, rulesResponse.getInlineRulesCount());
    assertEquals(
        EventType.EVENT_TYPE_NORMAL_DETECTION,
        rulesResponse.getInlineRules(0).getRule().getEffect().getEventType());
    assertEquals(createdRules.get(1).getId(), rulesResponse.getInlineRules(0).getRule().getId());
    assertEquals(
        0,
        rulesResponse
            .getInlineRules(0)
            .getRule()
            .getBlockingExpiryDetails()
            .getExpiryTimestampMillis());
    assertEquals(
        modsecDirectives
            + "SecRule REQUEST_HEADERS:Host|REQUEST_HEADERS:x-forwarded-host|REQUEST_HEADERS:forwarded \"@streq 127.0.0.1\" \"id:10000000,phase:2,capture,t:none,msg:'URL Regex and key-value conditions corresponding to custom signature rule - "
            + createdRules.get(1).getId()
            + "',logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1',tag:'rule-uuid/"
            + createdRules.get(1).getId()
            + "',severity:'CRITICAL',chain\"\n"
            + "SecRule REQUEST_HEADERS:x-real-ip \"@rx ^127\" \"capture,block,t:none\"",
        rulesResponse.getModsecRulesBlob());

    rulesResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                configServiceStub.getCustomSignatureModsecRules(
                    GetCustomSignatureModsecRulesRequest.newBuilder()
                        .setFilter(
                            GetRulesFilter.newBuilder()
                                .setDisabled(true)
                                .addRuleSources(RuleSource.RULE_SOURCE_CUSTOMER)
                                .build())
                        .build()));
    assertTrue(rulesResponse.getInlineRulesList().isEmpty());
    assertTrue(rulesResponse.getModsecRulesBlob().isEmpty());

    disableRule(createdRules.get(0));
    rulesResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                configServiceStub.getCustomSignatureModsecRules(
                    GetCustomSignatureModsecRulesRequest.newBuilder()
                        .setFilter(
                            GetRulesFilter.newBuilder()
                                .setDisabled(true)
                                .addRuleSources(RuleSource.RULE_SOURCE_CUSTOMER)
                                .build())
                        .build()));
    assertEquals(1, rulesResponse.getInlineRulesCount());
    assertTrue(rulesResponse.getInlineRules(0).getRule().getDisabled());
    assertEquals(createdRules.get(0).getId(), rulesResponse.getInlineRules(0).getRule().getId());
    assertEquals(
        0,
        rulesResponse
            .getInlineRules(0)
            .getRule()
            .getBlockingExpiryDetails()
            .getExpiryTimestampMillis());
    assertEquals(
        modsecDirectives
            + "SecRule REQUEST_HEADERS:Host|REQUEST_HEADERS:x-forwarded-host|REQUEST_HEADERS:forwarded \"@streq 127.0.0.1\" \"id:10000000,phase:2,capture,t:none,msg:'URL Regex and key-value conditions corresponding to custom signature rule - "
            + createdRules.get(0).getId()
            + "',logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1',tag:'rule-uuid/"
            + createdRules.get(0).getId()
            + "',severity:'CRITICAL',chain\"\n"
            + "SecRule REQUEST_HEADERS:x-real-ip \"@rx ^127\" \"capture,block,t:none\"",
        rulesResponse.getModsecRulesBlob());
  }

  @Test
  void testGetCustomSignatureEdgeDecisionRules() {
    // Create a compatible custom signature rule
    CustomSignatureRule createdRule =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                configServiceStub
                    .createCustomSignatureRule(
                        CreateCustomSignatureRuleRequest.newBuilder()
                            .setName("rule-1")
                            .setEffect(
                                RuleEffect.newBuilder()
                                    .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH))
                            .setDefinition(
                                RuleDefinition.newBuilder()
                                    .setClauseGroup(
                                        ClauseGroup.newBuilder()
                                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                                            .addClauses(
                                                Clause.newBuilder()
                                                    .setIpAddressExpression(
                                                        IpAddressExpression.newBuilder()
                                                            .addAllIpAddresses(
                                                                List.of("1.2.3.4", "2.3.4.5"))))))
                            .setRuleScope(RuleScope.getDefaultInstance())
                            .setRuleSource(RuleSource.RULE_SOURCE_CUSTOMER)
                            .build())
                    .getRule());
    // validate
    assertNotNull(createdRule);
    assertNotNull(createdRule.getId());
    // Get edge decision rules
    GetCustomSignatureEdgeDecisionRulesRequest edgeDecisionRulesRequest =
        GetCustomSignatureEdgeDecisionRulesRequest.newBuilder()
            .setRulesFilter(
                GetRulesFilter.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                    .addRuleSources(RuleSource.RULE_SOURCE_CUSTOMER))
            .build();
    GetCustomSignatureEdgeDecisionRulesResponse edgeDecisionRulesResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () -> configServiceStub.getCustomSignatureEdgeDecisionRules(edgeDecisionRulesRequest));
    // validate
    assertNotNull(edgeDecisionRulesResponse);
    assertNotNull(edgeDecisionRulesResponse.getEdgeDecisionEngineConfig());
    assertFalse(
        edgeDecisionRulesResponse.getEdgeDecisionEngineConfig().getDecisionRulesList().isEmpty());
    assertEquals(
        createdRule.getId(),
        edgeDecisionRulesResponse.getEdgeDecisionEngineConfig().getDecisionRules(0).getId());
    // Delete rule
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            configServiceStub.deleteCustomSignatureRule(
                DeleteCustomSignatureRuleRequest.newBuilder().setId(createdRule.getId()).build()));
  }

  private List<CustomSignatureRule> createRules() {
    RuleDefinition definition = getDefaultDefinition();

    CustomSignatureRule rule1 =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                configServiceStub
                    .createCustomSignatureRule(
                        CreateCustomSignatureRuleRequest.newBuilder()
                            .setName("rule-1")
                            .setEffect(
                                RuleEffect.newBuilder()
                                    .setEventType(EventType.EVENT_TYPE_TESTING_DETECTION)
                                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                                    .addEffects(
                                        getRuleEffectWithModificationsForInlineTracingAgent()))
                            .setDefinition(definition)
                            .setRuleScope(RuleScope.newBuilder().build())
                            .setRuleSource(RuleSource.RULE_SOURCE_CUSTOMER)
                            .build())
                    .getRule());

    CustomSignatureRule rule2 =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                configServiceStub
                    .createCustomSignatureRule(
                        CreateCustomSignatureRuleRequest.newBuilder()
                            .setName("rule-2")
                            .setEffect(
                                RuleEffect.newBuilder()
                                    .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
                                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                                    .addEffects(
                                        getRuleEffectWithModificationsForInlineTracingAgent()))
                            .setDefinition(definition)
                            .setRuleScope(RuleScope.newBuilder().build())
                            .setRuleSource(RuleSource.RULE_SOURCE_CUSTOMER)
                            .build())
                    .getRule());
    return List.of(rule1, rule2);
  }

  private CustomSignatureRule disableRule(CustomSignatureRule rule) {
    CustomSignatureRule updatedRule =
        CustomSignatureRule.newBuilder(rule.toBuilder().clearRuleSource().build())
            .setDisabled(true)
            .build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            configServiceStub
                .updateCustomSignatureRule(
                    UpdateCustomSignatureRuleRequest.newBuilder().setRule(updatedRule).build())
                .getRule());
  }

  private List<CustomSignatureRule> fetchAllRules() {
    return fetchAllRules(Collections.emptyList());
  }

  private List<CustomSignatureRule> fetchAllCreatedRules() {
    return fetchAllRules(List.of(RuleSource.RULE_SOURCE_CUSTOMER));
  }

  private List<CustomSignatureRule> fetchAllRules(List<RuleSource> ruleSources) {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            configServiceStub
                .getCustomSignatureRules(
                    GetCustomSignatureRulesRequest.newBuilder()
                        .setFilter(GetRulesFilter.newBuilder().addAllRuleSources(ruleSources))
                        .build())
                .getRulesList());
  }

  private RuleDefinition getDefaultDefinition() {
    return RuleDefinition.newBuilder()
        .setClauseGroup(
            ClauseGroup.newBuilder()
                .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                .addClauses(
                    Clause.newBuilder()
                        .setMatchExpression(
                            MatchExpression.newBuilder()
                                .setMatchKey(MatchKey.MATCH_KEY_HOST)
                                .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                .setMatchValue("127.0.0.1")
                                .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                                .build())
                        .build())
                .addClauses(
                    Clause.newBuilder()
                        .setKeyValueExpression(
                            KeyValueExpression.newBuilder()
                                .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                .setKeyMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                .setMatchKey("x-real-ip")
                                .setValueMatchOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                .setMatchValue("^127")
                                .build())
                        .build())
                .build())
        .build();
  }

  private List<CustomSignatureRule> getDefaultRules(String id1, String id2) {
    RuleDefinition definition = getDefaultDefinition();
    return List.of(
        CustomSignatureRule.newBuilder()
            .setId(id1)
            .setName("rule-1")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_TESTING_DETECTION)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                    .addAllRuleEvaluationPoints(
                        List.of(
                            RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM,
                            RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
                    .addEffects(getRuleEffectWithModificationsForInlineTracingAgent())
                    .build())
            .setDefinition(definition)
            .setRuleScope(RuleScope.newBuilder().build())
            .setRuleSource(RuleSource.RULE_SOURCE_CUSTOMER)
            .build(),
        CustomSignatureRule.newBuilder()
            .setId(id2)
            .setName("rule-2")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                    .addAllRuleEvaluationPoints(
                        List.of(
                            RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM,
                            RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
                    .addEffects(getRuleEffectWithModificationsForInlineTracingAgent())
                    .build())
            .setDefinition(definition)
            .setRuleScope(RuleScope.newBuilder().build())
            .setRuleSource(RuleSource.RULE_SOURCE_CUSTOMER)
            .build());
  }

  private void updateExpiryTime(CustomSignatureRule rule) {
    CustomSignatureRule updatedRule =
        CustomSignatureRule.newBuilder(rule.toBuilder().clearRuleSource().build())
            .setEffect(rule.getEffect().toBuilder().setEventType(EventType.EVENT_TYPE_ALLOW))
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder()
                    .setExpiryDuration(Duration.of(2, ChronoUnit.DAYS).toString())
                    .build())
            .build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            configServiceStub
                .updateCustomSignatureRule(
                    UpdateCustomSignatureRuleRequest.newBuilder().setRule(updatedRule).build())
                .getRule());
  }

  private List<CustomSignatureRule> fetchTestRules() {
    return fetchTestRules(EventType.EVENT_TYPE_TESTING_DETECTION);
  }

  private List<CustomSignatureRule> fetchTestRules(EventType eventType) {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            configServiceStub
                .getCustomSignatureRules(
                    GetCustomSignatureRulesRequest.newBuilder()
                        .setFilter(
                            GetRulesFilter.newBuilder()
                                .addEventTypes(eventType)
                                .addRuleSources(RuleSource.RULE_SOURCE_CUSTOMER)
                                .build())
                        .build())
                .getRulesList());
  }

  private RuleEffectWithModifications getRuleEffectWithModificationsForInlineTracingAgent() {
    return RuleEffectWithModifications.newBuilder()
        .setAgentRuleEffect(
            AgentRuleEffect.newBuilder()
                .addAgentModifications(
                    AgentModification.newBuilder()
                        .setHeaderInjection(
                            HeaderInjection.newBuilder()
                                .setHeaderName("header-name")
                                .setHeaderCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                                .setValue(
                                    FieldValue.newBuilder().setStaticValue("static-field-value")))))
        .build();
  }
}
