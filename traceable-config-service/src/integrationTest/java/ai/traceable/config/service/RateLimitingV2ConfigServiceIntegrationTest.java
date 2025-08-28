package ai.traceable.config.service;

import static ai.traceable.ratelimiting.config.service.v2.ApiAggregateType.API_AGGREGATE_TYPE_ACROSS_ENDPOINTS;
import static ai.traceable.ratelimiting.config.service.v2.ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.MatchOperator.MATCH_OPERATOR_EQUALS;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_CONTAIN;
import static ai.traceable.ratelimiting.config.service.v2.UserAggregateType.USER_AGGREGATE_TYPE_ACROSS_USERS;
import static ai.traceable.ratelimiting.config.service.v2.UserAggregateType.USER_AGGREGATE_TYPE_PER_USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.DeleteRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingEdgeDecisionRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingEdgeDecisionRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.IpAddressCondition;
import ai.traceable.ratelimiting.config.service.v2.IpAddressConditionType;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint;
import ai.traceable.ratelimiting.config.service.v2.RuleStatus;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.config.service.v2.TransactionActionConfig;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import com.google.common.io.Resources;
import com.google.protobuf.Value;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class RateLimitingV2ConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static RateLimitingConfigServiceBlockingStub rateLimitingConfigServiceBlockingStub;
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenantId");
  private static final String ruleName1 = "Default: DDoS Rate Limit Violation";
  private static final String ruleDescription1 =
      "High access rate detected which may result in denial of service";
  private static final String ruleId1 = "468a6645-2438-4268-89b3-d0ed93438d28";
  private static final String ruleName2 =
      "Default: Potential Access Rate Violation due to High Error Rate";
  private static final String ruleDescription2 =
      "High Error Rate detected which may mean potential Access Rate Violation";
  private static final String ruleId2 = "8b6542e3-e776-4430-8578-818bd59357c3";

  private static final RuleStatus RULE_SOURCE_TRACEABLE =
      RuleStatus.newBuilder()
          .setRuleCreationSource(RuleStatus.RuleSource.RULE_SOURCE_TRACEABLE)
          .build();
  private static final RuleStatus RULE_SOURCE_DEFAULT =
      RuleStatus.newBuilder()
          .setRuleCreationSource(RuleStatus.RuleSource.RULE_SOURCE_DEFAULT)
          .build();

  private static final RateLimitingRuleData.Builder ruleDataBuilder1 =
      RateLimitingRuleData.newBuilder()
          .setCategory(Category.CATEGORY_RATE_LIMITING)
          .setEnabled(false)
          .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
          .setCondition(
              Condition.newBuilder()
                  .setCompositeCondition(
                      CompositeCondition.newBuilder()
                          .setOperator(CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND)
                          .addChildren(
                              Condition.newBuilder()
                                  .setLeafCondition(
                                      LeafCondition.newBuilder()
                                          .setIpAddressCondition(
                                              IpAddressCondition.newBuilder()
                                                  .setIpAddressConditionType(
                                                      IpAddressConditionType
                                                          .IP_ADDRESS_CONDITION_TYPE_ALL_EXTERNAL))))))
          .addThresholdActionConfigs(
              ThresholdActionConfig.newBuilder()
                  .addResourceAccessThresholdConfigs(
                      ResourceAccessThresholdConfig.newBuilder()
                          .setUserAggregateType(USER_AGGREGATE_TYPE_ACROSS_USERS)
                          .setApiAggregateType(API_AGGREGATE_TYPE_ACROSS_ENDPOINTS)
                          .setRollingWindowThresholdConfig(
                              ResourceAccessThresholdConfig.RollingWindowThresholdConfig
                                  .newBuilder()
                                  .setCountAllowed(60000)
                                  .setDurationIso("PT60S")))
                  .addActions(
                      Action.newBuilder()
                          .setBlock(
                              Action.Block.newBuilder()
                                  .setEventSeverity(Action.EventSeverity.EVENT_SEVERITY_MEDIUM)
                                  .setUseThresholdDuration(true))));

  private static final RateLimitingRuleData.Builder ruleDataBuilder2 =
      RateLimitingRuleData.newBuilder()
          .setCategory(Category.CATEGORY_RATE_LIMITING)
          .setEnabled(false)
          .putAllLabels(Map.of("CWE", "285", "OWASP_API_2023", "API4"))
          .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
          .setCondition(
              Condition.newBuilder()
                  .setCompositeCondition(
                      CompositeCondition.newBuilder()
                          .setOperator(CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND)
                          .addChildren(
                              Condition.newBuilder()
                                  .setLeafCondition(
                                      LeafCondition.newBuilder()
                                          .setKeyValueCondition(
                                              KeyValueCondition.newBuilder()
                                                  .setType(KeyValueCondition.Type.TYPE_STATUS_CODE)
                                                  .setValueCondition(
                                                      KeyValueCondition.StringCondition.newBuilder()
                                                          .setValue("^(4|5)")
                                                          .setOperator(
                                                              MATCH_OPERATOR_MATCHES_REGEX)))))
                          .addChildren(
                              Condition.newBuilder()
                                  .setLeafCondition(
                                      LeafCondition.newBuilder()
                                          .setKeyValueCondition(
                                              KeyValueCondition.newBuilder()
                                                  .setType(KeyValueCondition.Type.TYPE_TAG)
                                                  .setKeyCondition(
                                                      KeyValueCondition.StringCondition.newBuilder()
                                                          .setValue("span.kind")
                                                          .setOperator(MATCH_OPERATOR_EQUALS))
                                                  .setValueCondition(
                                                      KeyValueCondition.StringCondition.newBuilder()
                                                          .setValue("server")
                                                          .setOperator(MATCH_OPERATOR_EQUALS)))))))
          .addThresholdActionConfigs(
              ThresholdActionConfig.newBuilder()
                  .addResourceAccessThresholdConfigs(
                      ResourceAccessThresholdConfig.newBuilder()
                          .setUserAggregateType(USER_AGGREGATE_TYPE_PER_USER)
                          .setApiAggregateType(API_AGGREGATE_TYPE_PER_ENDPOINT)
                          .setRollingWindowThresholdConfig(
                              ResourceAccessThresholdConfig.RollingWindowThresholdConfig
                                  .newBuilder()
                                  .setCountAllowed(25)
                                  .setDurationIso("PT300S"))
                          .build())
                  .addActions(
                      Action.newBuilder()
                          .setAlert(
                              Action.Alert.newBuilder()
                                  .setEventSeverity(Action.EventSeverity.EVENT_SEVERITY_MEDIUM))));

  @BeforeAll
  static void init() {
    rateLimitingConfigServiceBlockingStub =
        RateLimitingConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testCRUDRateLimitingRules() {
    // creating rule 1
    RateLimitingRuleData rateLimitingRuleData1 =
        ruleDataBuilder1
            .setName("rule1")
            .setDescription("description1")
            .setRuleStatus(RULE_SOURCE_TRACEABLE)
            .build();
    CreateRateLimitingRuleRequest createRequest1 =
        CreateRateLimitingRuleRequest.newBuilder().setData(rateLimitingRuleData1).build();
    RateLimitingRule rule1 =
        REQUEST_CONTEXT.call(
            () ->
                rateLimitingConfigServiceBlockingStub
                    .createRateLimitingRule(createRequest1)
                    .getRule());
    RateLimitingRule expectedRule1 =
        RateLimitingRule.newBuilder().setId(rule1.getId()).setData(rateLimitingRuleData1).build();
    assertEquals(expectedRule1, rule1);

    // creating rule 2
    RateLimitingRuleData rateLimitingRuleData2 =
        ruleDataBuilder2
            .setName("rule2")
            .setDescription("description2")
            .setRuleStatus(RULE_SOURCE_TRACEABLE)
            .build();
    CreateRateLimitingRuleRequest createRequest2 =
        CreateRateLimitingRuleRequest.newBuilder().setData(rateLimitingRuleData2).build();
    RateLimitingRule rule2 =
        REQUEST_CONTEXT.call(
            () ->
                rateLimitingConfigServiceBlockingStub
                    .createRateLimitingRule(createRequest2)
                    .getRule());
    RateLimitingRule expectedRule2 =
        RateLimitingRule.newBuilder().setId(rule2.getId()).setData(rateLimitingRuleData2).build();
    assertEquals(expectedRule2, rule2);

    // fetching rules without a filter
    List<RateLimitingRule> rules =
        getRules(GetRateLimitingRulesFilter.getDefaultInstance()); // default rules
    assertEquals(15, rules.size());
    /*
     * For the above couple of ruleData defined globally, ruleSource is set to DEFAULT but rate-limiting validator
     * ensures that if a rule is being created with the config-service stub call, then DEFAULT RuleSource can't be used.
     * Hence, the following 2 rules are being modified!
     */
    RateLimitingRule modifiedRule1 =
        RateLimitingRule.newBuilder()
            .setId(ruleId1)
            .setData(
                ruleDataBuilder1
                    .setName(ruleName1)
                    .setDescription(ruleDescription1)
                    .setRuleStatus(RULE_SOURCE_DEFAULT))
            .build();
    RateLimitingRule modifiedRule2 =
        RateLimitingRule.newBuilder()
            .setId(ruleId2)
            .setData(
                ruleDataBuilder2
                    .setName(ruleName2)
                    .setDescription(ruleDescription2)
                    .setRuleStatus(RULE_SOURCE_DEFAULT))
            .build();
    assertTrue(rules.contains(modifiedRule1));
    assertTrue(rules.contains(modifiedRule2));

    // fetching rules with a filter based on Category
    rules =
        getRules(
            GetRateLimitingRulesFilter.newBuilder()
                .addAllCategories(List.of(Category.CATEGORY_RATE_LIMITING))
                .build());
    assertTrue(rules.contains(modifiedRule1));
    assertTrue(rules.contains(modifiedRule2));

    // fetching rules with a filter based on RULE_EVALUATION_POINT
    rules =
        getRules(
            GetRateLimitingRulesFilter.newBuilder()
                .addAllRuleEvaluationPoints(
                    List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
                .build());
    assertFalse(rules.contains(modifiedRule1));
    assertTrue(rules.contains(modifiedRule2));

    // updating rule1
    RateLimitingRule updatedRule1 =
        rule1.toBuilder()
            .setData(
                rule1.getData().toBuilder()
                    .setName("updated-rule-1")
                    .setRuleStatus(RuleStatus.getDefaultInstance())
                    .build())
            .build();
    UpdateRateLimitingRuleRequest updateRequest =
        UpdateRateLimitingRuleRequest.newBuilder()
            .setRuleId(updatedRule1.getId())
            .setData(updatedRule1.getData())
            .build();
    RateLimitingRule actualUpdatedRule =
        REQUEST_CONTEXT
            .call(() -> rateLimitingConfigServiceBlockingStub.updateRateLimitingRule(updateRequest))
            .getRule();
    RateLimitingRule expectedUpdateRule =
        updatedRule1.toBuilder()
            .setData(updatedRule1.getData().toBuilder().setRuleStatus(RULE_SOURCE_TRACEABLE))
            .build();
    assertEquals(expectedUpdateRule, actualUpdatedRule);

    // deleting a rule
    CreateRateLimitingRuleRequest createRequest =
        CreateRateLimitingRuleRequest.newBuilder()
            .setData(
                RateLimitingRuleData.newBuilder()
                    .setName("rule3")
                    .setDescription("description3")
                    .setCategory(Category.CATEGORY_DATA_EXFILTRATION)
                    .setRuleStatus(RULE_SOURCE_TRACEABLE)
                    .setRuleConfigScope(
                        RuleConfigScope.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addEnvironmentIds("env-id")))
                    .setCondition(
                        Condition.newBuilder()
                            .setLeafCondition(
                                LeafCondition.newBuilder()
                                    .setScopeCondition(
                                        ScopeCondition.newBuilder()
                                            .setUrlScope(
                                                ScopeCondition.UrlScope.newBuilder()
                                                    .addUrlRegexes("url-regex")))))
                    .setTransactionActionConfig(
                        TransactionActionConfig.newBuilder()
                            .setAction(
                                Action.newBuilder()
                                    .setAllow(Action.Allow.newBuilder().setDurationIso("PT60S"))))
                    .addRuleEvaluationPoints(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
            .build();
    RateLimitingRule rateLimitingRule =
        REQUEST_CONTEXT
            .call(() -> rateLimitingConfigServiceBlockingStub.createRateLimitingRule(createRequest))
            .getRule();
    List<RateLimitingRule> rateLimitingRules =
        REQUEST_CONTEXT
            .call(
                () ->
                    rateLimitingConfigServiceBlockingStub.getRateLimitingRules(
                        GetRateLimitingRulesRequest.newBuilder()
                            .setRulesFilter(
                                GetRateLimitingRulesFilter.newBuilder()
                                    .addRuleEvaluationPoints(
                                        RuleEvaluationPoint
                                            .RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
                            .build()))
            .getRulesList();
    assertTrue(rateLimitingRules.contains(rateLimitingRule));
    String idOfRuleToBeDeleted = rateLimitingRule.getId();
    DeleteRateLimitingRuleRequest deleteRequest =
        DeleteRateLimitingRuleRequest.newBuilder().setRuleId(idOfRuleToBeDeleted).build();
    REQUEST_CONTEXT.call(
        () -> rateLimitingConfigServiceBlockingStub.deleteRateLimitingRule(deleteRequest));
    rateLimitingRules =
        REQUEST_CONTEXT
            .call(
                () ->
                    rateLimitingConfigServiceBlockingStub.getRateLimitingRules(
                        GetRateLimitingRulesRequest.newBuilder()
                            .setRulesFilter(
                                GetRateLimitingRulesFilter.newBuilder()
                                    .addRuleEvaluationPoints(
                                        RuleEvaluationPoint
                                            .RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
                            .build()))
            .getRulesList();
    assertFalse(rateLimitingRules.contains(rateLimitingRule));
  }

  @Test
  void testGetRateLimitingEdgeDecisionRules() {

    // Create a rate limiting rule that is compatible with edge decision
    RateLimitingRuleData rateLimitingRuleData =
        ruleDataBuilder1
            .setName("edge-decision-rule")
            .setDescription("Edge decision compatible rule")
            .setRuleStatus(RULE_SOURCE_TRACEABLE)
            .setEnabled(true)
            // Clear existing threshold action configs to ensure we have the right configuration
            .clearThresholdActionConfigs()
            // Add a threshold action config with a block action that has useThresholdDuration=true
            // This is required for edge decision compatibility
            .addThresholdActionConfigs(
                ThresholdActionConfig.newBuilder()
                    .addResourceAccessThresholdConfigs(
                        ResourceAccessThresholdConfig.newBuilder()
                            .setUserAggregateType(USER_AGGREGATE_TYPE_ACROSS_USERS)
                            .setApiAggregateType(API_AGGREGATE_TYPE_ACROSS_ENDPOINTS)
                            .setRollingWindowThresholdConfig(
                                ResourceAccessThresholdConfig.RollingWindowThresholdConfig
                                    .newBuilder()
                                    .setCountAllowed(1000)
                                    .setDurationIso("PT60S")))
                    .addActions(
                        Action.newBuilder()
                            .setBlock(
                                Action.Block.newBuilder()
                                    .setEventSeverity(Action.EventSeverity.EVENT_SEVERITY_MEDIUM)
                                    .setUseThresholdDuration(true))))
            .build();

    CreateRateLimitingRuleRequest createRequest =
        CreateRateLimitingRuleRequest.newBuilder().setData(rateLimitingRuleData).build();

    RateLimitingRule createdRule =
        REQUEST_CONTEXT.call(
            () ->
                rateLimitingConfigServiceBlockingStub
                    .createRateLimitingRule(createRequest)
                    .getRule());

    // Verify the rule was created
    RateLimitingRule expectedRule =
        RateLimitingRule.newBuilder()
            .setId(createdRule.getId())
            .setData(rateLimitingRuleData)
            .build();
    assertEquals(expectedRule, createdRule);

    // Get edge decision rules
    GetRateLimitingEdgeDecisionRulesRequest edgeDecisionRequest =
        GetRateLimitingEdgeDecisionRulesRequest.newBuilder()
            .setRulesFilter(GetRateLimitingRulesFilter.getDefaultInstance())
            .build();

    GetRateLimitingEdgeDecisionRulesResponse response =
        REQUEST_CONTEXT.call(
            () ->
                rateLimitingConfigServiceBlockingStub.getRateLimitingEdgeDecisionRules(
                    edgeDecisionRequest));

    // Verify that we got a non-empty EdgeDecisionEngineConfig
    assertNotNull(
        response.getEdgeDecisionEngineConfig(), "EdgeDecisionEngineConfig should not be null");

    // The MockFeatureFlagService has the "traceable-edge.edge-decision" flag set to true,
    // so we should get EdgeDecisionEngineConfig with decision rules
    assertTrue(
        response.getEdgeDecisionEngineConfig().getDecisionRulesCount() > 0,
        "EdgeDecisionEngineConfig should contain decision rules");

    // Clean up - delete the rule
    DeleteRateLimitingRuleRequest deleteRequest =
        DeleteRateLimitingRuleRequest.newBuilder().setRuleId(createdRule.getId()).build();
    REQUEST_CONTEXT.call(
        () -> rateLimitingConfigServiceBlockingStub.deleteRateLimitingRule(deleteRequest));
  }

  @Test
  void testGetRateLimitingRuleModsecRules() {
    String modsecDirectives = "";
    String modsecInitializationRules = "";
    URL directiveUrl =
        Resources.getResource(
            "modsec/crs/directives/modsec-directives-v3-secarglimits-detectiononly-mode.conf");
    URL initializationRulesUrl =
        Resources.getResource("modsec/crs/modsec-initialization-901-rules.conf");
    try {
      modsecDirectives = Resources.toString(directiveUrl, StandardCharsets.UTF_8) + "\n";
      modsecInitializationRules =
          Resources.toString(initializationRulesUrl, StandardCharsets.UTF_8);
    } catch (IOException e) {
      fail("Failed to read modsec directives file and/or modsec initialization rules file.");
    }

    GetRateLimitingRulesFilter getRateLimitingRulesFilter =
        GetRateLimitingRulesFilter.newBuilder()
            .addCategories(Category.CATEGORY_DATA_EXFILTRATION)
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)
            .setScope(
                RuleConfigScope.newBuilder()
                    .setEnvironmentScope(
                        EnvironmentScope.newBuilder()
                            .addEnvironmentIds(
                                "integration-test-get-rate-limiting-rule-modsec-rules-env")))
            .build();

    List<RateLimitingRule> createdRules = createModsecConvertibleRateLimitingRules();
    assertEquals(2, createdRules.size());

    GetRateLimitingRuleModsecRulesResponse getRateLimitingRuleModsecRulesResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                rateLimitingConfigServiceBlockingStub.getRateLimitingRuleModsecRules(
                    GetRateLimitingRuleModsecRulesRequest.newBuilder()
                        .setRulesFilter(
                            GetRateLimitingModsecRulesFilter.newBuilder()
                                .setRulesFilter(getRateLimitingRulesFilter))
                        .build()));

    List<String> createdRuleIds =
        createdRules.stream().map(RateLimitingRule::getId).collect(Collectors.toUnmodifiableList());

    assertEquals(2, getRateLimitingRuleModsecRulesResponse.getRulesCount());
    assertTrue(createdRuleIds.contains(getRateLimitingRuleModsecRulesResponse.getRules(0).getId()));
    assertTrue(createdRuleIds.contains(getRateLimitingRuleModsecRulesResponse.getRules(1).getId()));
    assertEquals(
        modsecDirectives + modsecInitializationRules,
        getRateLimitingRuleModsecRulesResponse.getModsecDirectivesBlob());

    disableCreatedRule(createdRules.get(0));

    getRateLimitingRuleModsecRulesResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                rateLimitingConfigServiceBlockingStub.getRateLimitingRuleModsecRules(
                    GetRateLimitingRuleModsecRulesRequest.newBuilder()
                        .setRulesFilter(
                            GetRateLimitingModsecRulesFilter.newBuilder()
                                .setRulesFilter(
                                    getRateLimitingRulesFilter.toBuilder()
                                        .setDisabled(true)
                                        .build()))
                        .build()));

    assertEquals(1, getRateLimitingRuleModsecRulesResponse.getRulesCount());
    assertEquals(
        createdRules.get(0).getId(), getRateLimitingRuleModsecRulesResponse.getRules(0).getId());
    assertEquals(
        modsecDirectives + modsecInitializationRules,
        getRateLimitingRuleModsecRulesResponse.getModsecDirectivesBlob());
  }

  private List<RateLimitingRule> createModsecConvertibleRateLimitingRules() {
    RuleConfigScope rateLimitingRuleConfigScope =
        RuleConfigScope.newBuilder()
            .setEnvironmentScope(
                EnvironmentScope.newBuilder()
                    .addEnvironmentIds("integration-test-get-rate-limiting-rule-modsec-rules-env"))
            .build();

    CreateRateLimitingRuleRequest createRateLimitingRuleRequest1 =
        CreateRateLimitingRuleRequest.newBuilder()
            .setData(
                RateLimitingRuleData.newBuilder()
                    .setCategory(Category.CATEGORY_DATA_EXFILTRATION)
                    .setName("name1")
                    .setDescription("description1")
                    .setRuleStatus(RULE_SOURCE_TRACEABLE)
                    .setRuleConfigScope(rateLimitingRuleConfigScope)
                    .setEnabled(true)
                    .setCondition(
                        Condition.newBuilder()
                            .setCompositeCondition(
                                CompositeCondition.newBuilder()
                                    .setOperator(
                                        CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND)
                                    .addChildren(
                                        Condition.newBuilder()
                                            .setLeafCondition(
                                                LeafCondition.newBuilder()
                                                    .setKeyValueCondition(
                                                        KeyValueCondition.newBuilder()
                                                            .setStaticValueCondition(
                                                                KeyValueCondition
                                                                    .StaticValueCondition
                                                                    .newBuilder()
                                                                    .setKeyCondition(
                                                                        KeyValueCondition
                                                                            .KeyCondition
                                                                            .newBuilder()
                                                                            .setKeyType(
                                                                                KeyValueCondition
                                                                                    .Type
                                                                                    .TYPE_USER_AGENT))
                                                                    .setValueMatchOperatorCondition(
                                                                        KeyValueCondition
                                                                            .MatchOperatorCondition
                                                                            .newBuilder()
                                                                            .setOperator(
                                                                                MATCH_OPERATOR_NOT_CONTAIN)
                                                                            .setValue(
                                                                                Value.newBuilder()
                                                                                    .setStringValue(
                                                                                        "not-contained-str-value")))))))
                                    .addChildren(
                                        Condition.newBuilder()
                                            .setLeafCondition(
                                                LeafCondition.newBuilder()
                                                    .setScopeCondition(
                                                        ScopeCondition.newBuilder()
                                                            .setUrlScope(
                                                                ScopeCondition.UrlScope.newBuilder()
                                                                    .addUrlRegexes(
                                                                        "url-regex")))))))
                    .setTransactionActionConfig(
                        TransactionActionConfig.newBuilder()
                            .setAction(
                                Action.newBuilder().setBlock(Action.Block.getDefaultInstance()))))
            .build();

    RateLimitingRule rule1 =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    rateLimitingConfigServiceBlockingStub.createRateLimitingRule(
                        createRateLimitingRuleRequest1))
            .getRule();

    CreateRateLimitingRuleRequest createRateLimitingRuleRequest2 =
        CreateRateLimitingRuleRequest.newBuilder()
            .setData(
                RateLimitingRuleData.newBuilder()
                    .setCategory(Category.CATEGORY_DATA_EXFILTRATION)
                    .setName("name2")
                    .setDescription("description2")
                    .setRuleStatus(RULE_SOURCE_TRACEABLE)
                    .setRuleConfigScope(rateLimitingRuleConfigScope)
                    .setEnabled(true)
                    .setCondition(
                        Condition.newBuilder()
                            .setLeafCondition(
                                LeafCondition.newBuilder()
                                    .setScopeCondition(
                                        ScopeCondition.newBuilder()
                                            .setUrlScope(
                                                ScopeCondition.UrlScope.newBuilder()
                                                    .addUrlRegexes("url-regex")))))
                    .setTransactionActionConfig(
                        TransactionActionConfig.newBuilder()
                            .setAction(
                                Action.newBuilder().setAllow(Action.Allow.getDefaultInstance()))))
            .build();

    RateLimitingRule rule2 =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    rateLimitingConfigServiceBlockingStub.createRateLimitingRule(
                        createRateLimitingRuleRequest2))
            .getRule();

    return List.of(rule1, rule2);
  }

  private void disableCreatedRule(RateLimitingRule rateLimitingRule) {
    RateLimitingRuleData updatedRuleData =
        rateLimitingRule.getData().toBuilder().setEnabled(false).clearRuleStatus().build();
    UpdateRateLimitingRuleRequest updateRequest =
        UpdateRateLimitingRuleRequest.newBuilder()
            .setRuleId(rateLimitingRule.getId())
            .setData(updatedRuleData)
            .build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () -> rateLimitingConfigServiceBlockingStub.updateRateLimitingRule(updateRequest));
  }

  private List<RateLimitingRule> getRules(GetRateLimitingRulesFilter getRateLimitingRulesFilter) {
    return GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                rateLimitingConfigServiceBlockingStub.getRateLimitingRules(
                    GetRateLimitingRulesRequest.newBuilder()
                        .setRulesFilter(getRateLimitingRulesFilter)
                        .build()))
        .getRulesList();
  }
}
