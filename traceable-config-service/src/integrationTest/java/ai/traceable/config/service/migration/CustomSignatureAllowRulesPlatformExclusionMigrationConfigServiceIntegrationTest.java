package ai.traceable.config.service.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.customsignature.config.service.v1.Category;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.customsignature.config.service.v1.RuleSource;
import com.google.protobuf.Value;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CustomSignatureAllowRulesPlatformExclusionMigrationConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub
      customSignatureConfigServiceBlockingStub;

  private static final String TENANT_ID = "tenant1";
  private static final List<RuleEvaluationPoint> allRuleEvaluationPointsList =
      List.of(
          RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM,
          RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE,
          RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT);

  @BeforeAll
  static void init() {
    customSignatureConfigServiceBlockingStub =
        CustomSignatureConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testMigrationForAllowRulesPlatformExclusion() {
    CreateCustomSignatureRuleRequest createCustomSignatureRuleRequest =
        getCustomSignatureRuleRequest();
    CustomSignatureRule createdRule =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    customSignatureConfigServiceBlockingStub.createCustomSignatureRule(
                        createCustomSignatureRuleRequest))
            .getRule();
    String createdRuleId = createdRule.getId();

    // before migration
    assertTrue(
        createdRule
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM));
    assertTrue(
        createdRule
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));
    assertTrue(
        createdRule
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT));

    List<CustomSignatureRule> allFetchedRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                customSignatureConfigServiceBlockingStub
                    .getCustomSignatureRules(
                        GetCustomSignatureRulesRequest.newBuilder()
                            .setFilter(
                                GetRulesFilter.newBuilder()
                                    .addAllRuleEvaluationPoints(allRuleEvaluationPointsList))
                            .build())
                    .getRulesList());

    // after migration
    CustomSignatureRule filteredCustomSignatureRule =
        allFetchedRules.stream()
            .filter(
                fetchedCustomSignatureRule ->
                    fetchedCustomSignatureRule.getId().equals(createdRuleId))
            .findAny()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Custom Signature rule with ID " + createdRuleId + " not found"));
    assertEquals(Category.CATEGORY_CUSTOM_SIGNATURE, filteredCustomSignatureRule.getCategory());
    assertTrue(
        filteredCustomSignatureRule
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));
    assertTrue(
        filteredCustomSignatureRule
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT));
    assertFalse(
        filteredCustomSignatureRule
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM));
  }

  private CreateCustomSignatureRuleRequest getCustomSignatureRuleRequest() {
    return CreateCustomSignatureRuleRequest.newBuilder()
        .setName("rule-name")
        .setDescription("rule-description")
        .setDefinition(
            RuleDefinition.newBuilder()
                .setClauseGroup(
                    ClauseGroup.newBuilder()
                        .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                        .addClauses(
                            Clause.newBuilder()
                                .setMatchExpression(
                                    MatchExpression.newBuilder()
                                        .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                                        .setMatchKey(MatchKey.MATCH_KEY_URL)
                                        .setMatchOperator(MatchOperator.MATCH_OPERATOR_CONTAINS)
                                        .setValue(
                                            Value.newBuilder().setStringValue("url-str-val"))))))
        .setEffect(
            RuleEffect.newBuilder()
                .setEventType(EventType.EVENT_TYPE_ALLOW)
                .addAllRuleEvaluationPoints(allRuleEvaluationPointsList))
        .setRuleScope(
            RuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-id")))
        .setRuleSource(RuleSource.RULE_SOURCE_TRACEABLE)
        .build();
  }
}
