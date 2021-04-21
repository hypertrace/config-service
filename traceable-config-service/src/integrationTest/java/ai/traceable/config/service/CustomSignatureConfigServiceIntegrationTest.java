package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CustomSignatureConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static CustomSignatureConfigServiceBlockingStub configServiceStub;

  @BeforeAll
  static void init() {
    configServiceStub =
        CustomSignatureConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  public void testCreateRule() {
    assertTrue(fetchAllRules().isEmpty());
    List<CustomSignatureRule> createdRules = createDefaultRules();
    List<CustomSignatureRule> defaultRules =
        getDefaultRules(createdRules.get(0).getId(), createdRules.get(1).getId());
    assertEquals(defaultRules.get(0), createdRules.get(0));
    assertEquals(defaultRules.get(1), createdRules.get(1));
    assertEquals(2, fetchAllRules().size());
  }

  @Test()
  public void testUpdateRule() {
    assertTrue(fetchAllRules().isEmpty());
    List<CustomSignatureRule> createdRules = createDefaultRules();
    List<CustomSignatureRule> fetchedRules = fetchAllRules();
    assertEquals(2, fetchedRules.size());
    assertFalse(fetchedRules.get(0).getDisabled());
    assertFalse(fetchedRules.get(1).getDisabled());

    assertEquals(
        CustomSignatureRule.newBuilder(createdRules.get(0)).setDisabled(true).build(),
        disableRule(createdRules.get(0)));

    fetchedRules = fetchAllRules();
    assertEquals(2, fetchedRules.size());
    if (fetchedRules.get(0).getId().equals(createdRules.get(0))) {
      assertTrue(fetchedRules.get(0).getDisabled());
      assertFalse(fetchedRules.get(1).getDisabled());
    } else {
      assertTrue(fetchedRules.get(1).getDisabled());
      assertFalse(fetchedRules.get(0).getDisabled());
    }
  }

  @Test()
  public void testDeleteRule() {
    assertTrue(fetchAllRules().isEmpty());
    List<CustomSignatureRule> createdRules = createDefaultRules();
    assertEquals(2, fetchAllRules().size());
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            configServiceStub.deleteCustomSignatureRule(
                DeleteCustomSignatureRuleRequest.newBuilder()
                    .setId(createdRules.get(1).getId())
                    .build()));
    List<CustomSignatureRule> fetchedRules = fetchAllRules();
    assertEquals(1, fetchedRules.size());
    assertEquals(createdRules.get(0).getId(), fetchedRules.get(0).getId());
  }

  @Test
  public void testGetRules() {
    assertTrue(fetchAllRules().isEmpty());
    List<CustomSignatureRule> createdRules = createDefaultRules();
    assertEquals(2, fetchAllRules().size());

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
                            .setFilter(GetRulesFilter.newBuilder().setDisabled(true).build())
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
                            .setFilter(GetRulesFilter.newBuilder().setDisabled(true).build())
                            .build())
                    .getRulesList());
    assertEquals(1, fetchedRules.size());
    assertEquals(true, fetchedRules.get(0).getDisabled());
    assertEquals(createdRules.get(0).getId(), fetchedRules.get(0).getId());
  }

  private List<CustomSignatureRule> createDefaultRules() {
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
                                    .setEventType(EventType.EVENT_TYPE_TENTATIVE_DETECTION)
                                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                                    .build())
                            .setDefinition(definition)
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
                                    .build())
                            .setDefinition(definition)
                            .build())
                    .getRule());
    return List.of(rule1, rule2);
  }

  private CustomSignatureRule disableRule(CustomSignatureRule rule) {
    CustomSignatureRule updatedRule =
        CustomSignatureRule.newBuilder(rule).setDisabled(true).build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            configServiceStub
                .updateCustomSignatureRule(
                    UpdateCustomSignatureRuleRequest.newBuilder().setRule(updatedRule).build())
                .getRule());
  }

  private List<CustomSignatureRule> fetchAllRules() {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            configServiceStub
                .getCustomSignatureRules(GetCustomSignatureRulesRequest.newBuilder().build())
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
                    .setEventType(EventType.EVENT_TYPE_TENTATIVE_DETECTION)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                    .build())
            .setDefinition(definition)
            .build(),
        CustomSignatureRule.newBuilder()
            .setId(id2)
            .setName("rule-2")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                    .build())
            .setDefinition(definition)
            .build());
  }
}
