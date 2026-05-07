package ai.traceable.config.service.migration;

import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_REQUEST_COOKIE;
import static ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT;
import static ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.DataLocation;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition;
import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint;
import ai.traceable.ratelimiting.config.service.v2.RuleStatus;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;
import ai.traceable.ratelimiting.config.service.v2.TransactionActionConfig;
import com.google.protobuf.Value;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class RateLimitingAllowRulesPlatformExclusionMigrationConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub
      rateLimitingConfigServiceBlockingStub;

  private static final String TENANT_ID_1 = "tenant-id-1";
  private static final String TENANT_ID_2 = "tenant-id-2";
  private static final String ENV_ID_1 = "env-id-1";
  private static final String ENV_ID_2 = "env-id-2";

  @BeforeAll
  static void init() {
    rateLimitingConfigServiceBlockingStub =
        RateLimitingConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testMigrationForAllowRulesPlatformExclusion() {
    // case-1: rule has rule evaluation points other than PLATFORM
    CreateRateLimitingRuleRequest createRateLimitingRuleRequest1 =
        getCreateRateLimitingRuleRequest1();
    RateLimitingRule createdRule1 =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID_1,
                () ->
                    rateLimitingConfigServiceBlockingStub.createRateLimitingRule(
                        createRateLimitingRuleRequest1))
            .getRule();
    String createdRuleId1 = createdRule1.getId();

    // before migration
    assertRuleEvaluationPoints(
        createdRule1.getData(),
        RULE_EVALUATION_POINT_PLATFORM,
        RULE_EVALUATION_POINT_INLINE_TRACING_AGENT);

    List<RateLimitingRule> allFetchedRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID_1,
            () ->
                rateLimitingConfigServiceBlockingStub
                    .getRateLimitingRules(GetRateLimitingRulesRequest.getDefaultInstance())
                    .getRulesList());

    // after migration
    RateLimitingRule filteredRateLimitingRule1 =
        allFetchedRules.stream()
            .filter(rateLimitingRule -> rateLimitingRule.getId().equals(createdRuleId1))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Request-based DLP rule with ID " + createdRuleId1 + " not found"));
    ;

    assertRuleEvaluationPoints(
        filteredRateLimitingRule1.getData(), RULE_EVALUATION_POINT_INLINE_TRACING_AGENT);

    // case-2: rule has no rule evaluation points other than PLATFORM
    CreateRateLimitingRuleRequest createRateLimitingRuleRequest2 =
        getCreateRateLimitingRuleRequest2();
    RateLimitingRule createdRule2 =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID_2,
                () ->
                    rateLimitingConfigServiceBlockingStub.createRateLimitingRule(
                        createRateLimitingRuleRequest2))
            .getRule();
    String createdRuleId2 = createdRule2.getId();

    // before migration
    assertRuleEvaluationPoints(createdRule2.getData(), RULE_EVALUATION_POINT_PLATFORM);

    allFetchedRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID_2,
            () ->
                rateLimitingConfigServiceBlockingStub
                    .getRateLimitingRules(GetRateLimitingRulesRequest.getDefaultInstance())
                    .getRulesList());

    // after migration
    RateLimitingRule filteredRateLimitingRule2 =
        allFetchedRules.stream()
            .filter(rateLimitingRule -> rateLimitingRule.getId().equals(createdRuleId2))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Request-based DLP rule with ID " + createdRuleId2 + " not found"));
    ;

    assertRuleEvaluationPoints(
        filteredRateLimitingRule2.getData(), RULE_EVALUATION_POINT_INLINE_TRACING_AGENT);
  }

  private CreateRateLimitingRuleRequest getCreateRateLimitingRuleRequest1() {
    return CreateRateLimitingRuleRequest.newBuilder()
        .setData(
            RateLimitingRuleData.newBuilder()
                .setName("rule-name-1")
                .setDescription("rule-description-1")
                .setCategory(Category.CATEGORY_DATA_EXFILTRATION)
                .setEnabled(true)
                .setTransactionActionConfig(getTransactionActionConfig())
                .setCondition(getRateLimitingRuleCondition())
                .setRuleConfigScope(getRuleConfigScope(ENV_ID_1))
                .setRuleStatus(getRuleStatus())
                .addAllRuleEvaluationPoints(
                    List.of(
                        RULE_EVALUATION_POINT_INLINE_TRACING_AGENT,
                        RULE_EVALUATION_POINT_PLATFORM)))
        .build();
  }

  private CreateRateLimitingRuleRequest getCreateRateLimitingRuleRequest2() {
    return CreateRateLimitingRuleRequest.newBuilder()
        .setData(
            RateLimitingRuleData.newBuilder()
                .setName("rule-name-2")
                .setDescription("rule-description-2")
                .setCategory(Category.CATEGORY_DATA_EXFILTRATION)
                .setEnabled(true)
                .setTransactionActionConfig(getTransactionActionConfig())
                .setCondition(getRateLimitingRuleCondition())
                .setRuleConfigScope(getRuleConfigScope(ENV_ID_2))
                .setRuleStatus(getRuleStatus())
                .addAllRuleEvaluationPoints(List.of(RULE_EVALUATION_POINT_PLATFORM)))
        .build();
  }

  private TransactionActionConfig getTransactionActionConfig() {
    return TransactionActionConfig.newBuilder()
        .setAction(Action.newBuilder().setAllow(Action.Allow.getDefaultInstance()))
        .build();
  }

  private Condition getRateLimitingRuleCondition() {
    return Condition.newBuilder()
        .setCompositeCondition(
            CompositeCondition.newBuilder()
                .setOperator(CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND)
                .addChildren(
                    Condition.newBuilder()
                        .setLeafCondition(
                            LeafCondition.newBuilder()
                                .setScopeCondition(
                                    ScopeCondition.newBuilder()
                                        .setUrlScope(
                                            ScopeCondition.UrlScope.newBuilder()
                                                .addUrlRegexes("url-regex")))))
                .addChildren(
                    Condition.newBuilder()
                        .setLeafCondition(
                            LeafCondition.newBuilder()
                                .setDatatypeCondition(getDatatypeCondition()))))
        .build();
  }

  private RuleConfigScope getRuleConfigScope(String envId) {
    return RuleConfigScope.newBuilder()
        .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds(envId))
        .build();
  }

  private RuleStatus getRuleStatus() {
    return RuleStatus.newBuilder()
        .setRuleCreationSource(RuleStatus.RuleSource.RULE_SOURCE_CUSTOMER)
        .build();
  }

  private DatatypeCondition getDatatypeCondition() {
    return DatatypeCondition.newBuilder()
        .setDataLocation(DataLocation.DATA_LOCATION_REQUEST)
        .addDatasetIds("dataset-id")
        .setDatatypeMatching(
            DatatypeCondition.DatatypeMatching.newBuilder()
                .setRegexBasedMatching(
                    DatatypeCondition.RegexBasedMatching.newBuilder()
                        .setCustomMatchingLocation(
                            KeyValueCondition.newBuilder()
                                .setStaticValueCondition(
                                    KeyValueCondition.StaticValueCondition.newBuilder()
                                        .setKeyCondition(
                                            KeyValueCondition.KeyCondition.newBuilder()
                                                .setKeyType(TYPE_REQUEST_COOKIE)
                                                .setKeyMatchOperatorCondition(
                                                    KeyValueCondition.MatchOperatorCondition
                                                        .newBuilder()
                                                        .setOperator(
                                                            KeyValueCondition.MatchOperator
                                                                .MATCH_OPERATOR_EQUALS)
                                                        .setValue(
                                                            Value.newBuilder()
                                                                .setStringValue(
                                                                    "cookie-str-val"))))))))
        .build();
  }

  private void assertRuleEvaluationPoints(
      RateLimitingRuleData rateLimitingRuleData,
      RuleEvaluationPoint... expectedRuleEvaluationPoints) {
    List<RuleEvaluationPoint> actualRuleEvaluationPoints =
        rateLimitingRuleData.getRuleEvaluationPointsList();
    assertEquals(expectedRuleEvaluationPoints.length, actualRuleEvaluationPoints.size());
    for (RuleEvaluationPoint expectedRuleEvaluationPoint : expectedRuleEvaluationPoints) {
      assertTrue(
          actualRuleEvaluationPoints.contains(expectedRuleEvaluationPoint),
          "Expected ruleEvaluationPoints to contain: "
              + expectedRuleEvaluationPoint
              + " but was: "
              + actualRuleEvaluationPoints);
    }
  }
}
