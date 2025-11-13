package ai.traceable.config.service.migration;

import static ai.traceable.ratelimiting.config.service.v2.ApiAggregateType.API_AGGREGATE_TYPE_ACROSS_ENDPOINTS;
import static ai.traceable.ratelimiting.config.service.v2.ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.MatchOperator.MATCH_OPERATOR_EQUALS;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
import static ai.traceable.ratelimiting.config.service.v2.UserAggregateType.USER_AGGREGATE_TYPE_ACROSS_USERS;
import static ai.traceable.ratelimiting.config.service.v2.UserAggregateType.USER_AGGREGATE_TYPE_PER_USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.RuleStatus;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesStore;
import com.google.protobuf.Value;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RateLimitingMigrationConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub
      rateLimitingConfigServiceBlockingStub;

  private static RateLimitingRulesStore rateLimitingRulesStore;

  private static final String TENANT_ID = "tenant1";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final String APPLICATION_CONFIG =
      "configs/traceable-config-service/application.conf";

  @BeforeAll
  static void init() {
    rateLimitingConfigServiceBlockingStub =
        RateLimitingConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    ConfigChangeEventGenerator configChangeEventGenerator =
        new ConfigChangeEventGenerator() {
          @Override
          public void sendCreateNotification(
              RequestContext requestContext, String configType, Value config) {
            // no-op for tests
          }

          @Override
          public void sendDeleteNotification(
              RequestContext requestContext, String configType, Value config) {
            // no-op for tests
          }

          @Override
          public void sendUpdateNotification(
              RequestContext requestContext,
              String configType,
              Value prevConfig,
              Value latestConfig) {
            // no-op for tests
          }

          @Override
          public void sendCreateNotification(
              RequestContext requestContext, String configType, String context, Value config) {
            // no-op for tests
          }

          @Override
          public void sendDeleteNotification(
              RequestContext requestContext, String configType, String context, Value config) {
            // no-op for tests
          }

          @Override
          public void sendUpdateNotification(
              RequestContext requestContext,
              String configType,
              String context,
              Value prevConfig,
              Value latestConfig) {
            // no-op for tests
          }
        };

    RateLimitingConfigServiceConfig rateLimitingConfigServiceConfig =
        new RateLimitingConfigServiceConfig(
            ConfigFactory.parseURL(
                Objects.requireNonNull(
                    RateLimitingMigrationConfigServiceIntegrationTest.class
                        .getClassLoader()
                        .getResource(APPLICATION_CONFIG))));

    rateLimitingRulesStore =
        new RateLimitingRulesStore(
            configServiceBlockingStub, configChangeEventGenerator, rateLimitingConfigServiceConfig);
  }

  @Test
  void testMigrationForRuleEvaluationPoints() {
    RuleConfigScope ruleConfigScope =
        RuleConfigScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-id"))
            .build();

    RateLimitingRuleData.Builder ruleDataBuilder1 =
        RateLimitingRuleData.newBuilder()
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(false)
            .addAllRuleEvaluationPoints(
                List.of(
                    ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint
                        .RULE_EVALUATION_POINT_EDGE,
                    ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint
                        .RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
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
                                                ai.traceable.ratelimiting.config.service.v2
                                                    .IpAddressCondition.newBuilder()
                                                    .setIpAddressConditionType(
                                                        ai.traceable.ratelimiting.config.service.v2
                                                            .IpAddressConditionType
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

    RateLimitingRuleData.Builder ruleDataBuilder2 =
        RateLimitingRuleData.newBuilder()
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setEnabled(false)
            .putAllLabels(Map.of("CWE", "285", "OWASP_API_2023", "API4"))
            .addRuleEvaluationPoints(
                ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint
                    .RULE_EVALUATION_POINT_PLATFORM)
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
                                                    .setType(
                                                        KeyValueCondition.Type.TYPE_STATUS_CODE)
                                                    .setValueCondition(
                                                        KeyValueCondition.StringCondition
                                                            .newBuilder()
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
                                                        KeyValueCondition.StringCondition
                                                            .newBuilder()
                                                            .setValue("span.kind")
                                                            .setOperator(MATCH_OPERATOR_EQUALS))
                                                    .setValueCondition(
                                                        KeyValueCondition.StringCondition
                                                            .newBuilder()
                                                            .setValue("server")
                                                            .setOperator(
                                                                MATCH_OPERATOR_EQUALS)))))))
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
                                    .setEventSeverity(
                                        Action.EventSeverity.EVENT_SEVERITY_MEDIUM))));

    RuleStatus TRACEABLE_RULE_STATUS =
        RuleStatus.newBuilder()
            .setRuleCreationSource(RuleStatus.RuleSource.RULE_SOURCE_TRACEABLE)
            .build();

    /*
     * creating a rule without RuleEvaluationPoints - expectation is that migration should happen
     */
    RateLimitingRuleData createRateLimitingRuleData1 =
        ruleDataBuilder2
            .setName("create-ratelimiting-rule-name-1")
            .setDescription("create-ratelimiting-rule-description-1")
            .setRuleStatus(TRACEABLE_RULE_STATUS)
            .setRuleConfigScope(ruleConfigScope)
            .clearRuleEvaluationPoints()
            .build();

    RateLimitingRule createdRule1 =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    rateLimitingConfigServiceBlockingStub.createRateLimitingRule(
                        CreateRateLimitingRuleRequest.newBuilder()
                            .setData(createRateLimitingRuleData1)
                            .build()))
            .getRule();

    /*
     * The list of RuleEvaluationPoints is expected to be non-empty here as the creation request has gotten migrated
     */
    assertTrue(
        createdRule1
            .getData()
            .getRuleEvaluationPointsList()
            .contains(
                ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint
                    .RULE_EVALUATION_POINT_PLATFORM));

    /*
     * This verifies that when migration occurs for a rule containing a blocking action in its threshold configuration
     * and with the useThresholdDuration flag set to true,
     * the PLATFORM evaluation point should be excluded from the rule's evaluation points.
     */
    RateLimitingRuleData createRateLimitingRuleData2 =
        ruleDataBuilder1
            .setName("create-ratelimiting-rule-name-2")
            .setDescription("create-ratelimiting-rule-description-2")
            .setRuleStatus(TRACEABLE_RULE_STATUS)
            .setRuleConfigScope(ruleConfigScope)
            .clearRuleEvaluationPoints()
            .build();

    RateLimitingRule createdRule2 =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    rateLimitingConfigServiceBlockingStub.createRateLimitingRule(
                        CreateRateLimitingRuleRequest.newBuilder()
                            .setData(createRateLimitingRuleData2)
                            .build()))
            .getRule();

    /*
     * The list of RuleEvaluationPoints is expected to be non-empty, but with a RuleEvaluationPoint other than PLATFORM
     */
    assertTrue(
        createdRule2
            .getData()
            .getRuleEvaluationPointsList()
            .contains(
                ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint
                    .RULE_EVALUATION_POINT_EDGE));
    assertFalse(
        createdRule2
            .getData()
            .getRuleEvaluationPointsList()
            .contains(
                ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint
                    .RULE_EVALUATION_POINT_PLATFORM));

    /*
     * Proceeding with a similar approach for update rule flow
     */
    RateLimitingRuleData updateRateLimitingRuleData1 =
        ruleDataBuilder2
            .setName("update-ratelimiting-rule-name-1")
            .setDescription("update-ratelimiting-rule-description-1")
            .setRuleConfigScope(ruleConfigScope)
            .setRuleStatus(RuleStatus.getDefaultInstance())
            .clearRuleEvaluationPoints()
            .build();
    RateLimitingRule updatedRule1 =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    rateLimitingConfigServiceBlockingStub.updateRateLimitingRule(
                        UpdateRateLimitingRuleRequest.newBuilder()
                            .setData(updateRateLimitingRuleData1)
                            .setRuleId(createdRule1.getId())
                            .build()))
            .getRule();
    assertTrue(
        updatedRule1
            .getData()
            .getRuleEvaluationPointsList()
            .contains(
                ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint
                    .RULE_EVALUATION_POINT_PLATFORM));

    RateLimitingRuleData updateRateLimitingRuleData2 =
        ruleDataBuilder1
            .setName("update-ratelimiting-rule-name-2")
            .setDescription("update-ratelimiting-rule-description-2")
            .setRuleConfigScope(ruleConfigScope)
            .setRuleStatus(RuleStatus.getDefaultInstance())
            .clearRuleEvaluationPoints()
            .build();
    RateLimitingRule updatedRule2 =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    rateLimitingConfigServiceBlockingStub.updateRateLimitingRule(
                        UpdateRateLimitingRuleRequest.newBuilder()
                            .setData(updateRateLimitingRuleData2)
                            .setRuleId(createdRule2.getId())
                            .build()))
            .getRule();
    assertTrue(
        updatedRule2
            .getData()
            .getRuleEvaluationPointsList()
            .contains(
                ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint
                    .RULE_EVALUATION_POINT_EDGE));
    assertFalse(
        updatedRule2
            .getData()
            .getRuleEvaluationPointsList()
            .contains(
                ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint
                    .RULE_EVALUATION_POINT_PLATFORM));

    /*
     * Check for getRateLimitingRules flow - all rules without RuleEvaluationPoints are properly migrated.
     */
    upsertRateLimitingRuleWithoutRuleEvaluationPoints();
    List<RateLimitingRule> fetchedRules = fetchAllRateLimitingRules();

    // 14 default rules + 2 updated rules + 1 rule without any rule evaluation point initially
    assertEquals(17, fetchedRules.size());

    for (RateLimitingRule rateLimitingRule : fetchedRules) {
      assertFalse(rateLimitingRule.getData().getRuleEvaluationPointsList().isEmpty());
    }
  }

  private List<RateLimitingRule> fetchAllRateLimitingRules() {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            rateLimitingConfigServiceBlockingStub
                .getRateLimitingRules(GetRateLimitingRulesRequest.getDefaultInstance())
                .getRulesList());
  }

  private void upsertRateLimitingRuleWithoutRuleEvaluationPoints() {
    String rateLimitingRuleId = UUID.randomUUID().toString();
    RateLimitingRuleData rateLimitingRuleData =
        RateLimitingRuleData.newBuilder()
            .setName("rate-limiting-rule-with-no-rule-evaluation-points")
            .setCategory(Category.CATEGORY_RATE_LIMITING)
            .setDescription("description")
            .setRuleStatus(
                RuleStatus.newBuilder()
                    .setRuleCreationSource(RuleStatus.RuleSource.RULE_SOURCE_TRACEABLE))
            .setRuleConfigScope(
                RuleConfigScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-id")))
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
                                    .setDurationIso("PT300S")))
                    .addActions(
                        Action.newBuilder()
                            .setAllow(Action.Allow.newBuilder().setDurationIso("PT500S"))))
            .setCondition(
                Condition.newBuilder()
                    .setLeafCondition(
                        LeafCondition.newBuilder()
                            .setScopeCondition(
                                ScopeCondition.newBuilder()
                                    .setUrlScope(
                                        ScopeCondition.UrlScope.newBuilder()
                                            .addUrlRegexes("url-regex")))))
            .build();
    RateLimitingRule rateLimitingRule =
        RateLimitingRule.newBuilder()
            .setData(rateLimitingRuleData)
            .setId(rateLimitingRuleId)
            .build();

    rateLimitingRulesStore.upsertObjects(REQUEST_CONTEXT, List.of(rateLimitingRule));
  }
}
