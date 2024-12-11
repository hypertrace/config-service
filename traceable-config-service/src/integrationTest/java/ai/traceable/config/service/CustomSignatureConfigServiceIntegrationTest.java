package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
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
        CustomSignatureConfigServiceGrpc.newBlockingStub(channelForInternalServices)
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
    assertTrue(fetchedRules.get(0).getDisabled());
    assertEquals(createdRules.get(0).getId(), fetchedRules.get(0).getId());

    fetchedRules = fetchTestRules();
    assertFalse(fetchedRules.get(0).getBlockingExpiryDetails().hasExpiryDuration());
    assertEquals(0, fetchedRules.get(0).getBlockingExpiryDetails().getExpiryTimestampMillis());
    updateExpiryTime(fetchedRules.get(0));
    List<CustomSignatureRule> updatedRules = fetchTestRules();
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

    assertTrue(fetchAllRules().isEmpty());
    List<CustomSignatureRule> createdRules = createDefaultRules();
    assertEquals(2, fetchAllRules().size());

    GetCustomSignatureModsecRulesResponse rulesResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                configServiceStub.getCustomSignatureModsecRules(
                    GetCustomSignatureModsecRulesRequest.newBuilder()
                        .setFilter(
                            GetRulesFilter.newBuilder()
                                .addEventTypes(EventType.EVENT_TYPE_NORMAL_DETECTION)
                                .build())
                        .build()));

    assertEquals(1, rulesResponse.getRulesCount());
    assertEquals(
        EventType.EVENT_TYPE_NORMAL_DETECTION,
        rulesResponse.getRules(0).getEffect().getEventType());
    assertEquals(createdRules.get(1).getId(), rulesResponse.getRules(0).getId());
    assertEquals(
        0, rulesResponse.getRules(0).getBlockingExpiryDetails().getExpiryTimestampMillis());
    assertEquals(
        modsecDirectives
            + "SecRule REQUEST_HEADERS:Host|REQUEST_HEADERS:x-forwarded-host|REQUEST_HEADERS:forwarded \"@streq 127.0.0.1\" \"id:10000001,phase:2,capture,t:none,msg:'rule-2',logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1',tag:'rule-uuid/"
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
                        .setFilter(GetRulesFilter.newBuilder().setDisabled(true).build())
                        .build()));
    assertTrue(rulesResponse.getRulesList().isEmpty());
    assertTrue(rulesResponse.getModsecRulesBlob().isEmpty());

    disableRule(createdRules.get(0));
    rulesResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                configServiceStub.getCustomSignatureModsecRules(
                    GetCustomSignatureModsecRulesRequest.newBuilder()
                        .setFilter(GetRulesFilter.newBuilder().setDisabled(true).build())
                        .build()));
    assertEquals(1, rulesResponse.getRulesCount());
    assertTrue(rulesResponse.getRules(0).getDisabled());
    assertEquals(createdRules.get(0).getId(), rulesResponse.getRules(0).getId());
    assertEquals(
        0, rulesResponse.getRules(0).getBlockingExpiryDetails().getExpiryTimestampMillis());
    assertEquals(
        modsecDirectives
            + "SecRule REQUEST_HEADERS:Host|REQUEST_HEADERS:x-forwarded-host|REQUEST_HEADERS:forwarded \"@streq 127.0.0.1\" \"id:10000001,phase:2,capture,t:none,msg:'rule-1',logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1',tag:'rule-uuid/"
            + createdRules.get(0).getId()
            + "',severity:'CRITICAL',chain\"\n"
            + "SecRule REQUEST_HEADERS:x-real-ip \"@rx ^127\" \"capture,block,t:none\"",
        rulesResponse.getModsecRulesBlob());
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
                                    .setEventType(EventType.EVENT_TYPE_TESTING_DETECTION)
                                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                                    .build())
                            .setDefinition(definition)
                            .setRuleScope(RuleScope.newBuilder().build())
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
                            .setRuleScope(RuleScope.newBuilder().build())
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
                    .setEventType(EventType.EVENT_TYPE_TESTING_DETECTION)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                    .build())
            .setDefinition(definition)
            .setRuleScope(RuleScope.newBuilder().build())
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
            .setRuleScope(RuleScope.newBuilder().build())
            .build());
  }

  private CustomSignatureRule updateExpiryTime(CustomSignatureRule rule) {
    CustomSignatureRule updatedRule =
        CustomSignatureRule.newBuilder(rule)
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder()
                    .setExpiryDuration(Duration.of(2, ChronoUnit.DAYS).toString())
                    .build())
            .build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            configServiceStub
                .updateCustomSignatureRule(
                    UpdateCustomSignatureRuleRequest.newBuilder().setRule(updatedRule).build())
                .getRule());
  }

  private List<CustomSignatureRule> fetchTestRules() {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            configServiceStub
                .getCustomSignatureRules(
                    GetCustomSignatureRulesRequest.newBuilder()
                        .setFilter(
                            GetRulesFilter.newBuilder()
                                .addEventTypes(EventType.EVENT_TYPE_TESTING_DETECTION)
                                .build())
                        .build())
                .getRulesList());
  }
}
