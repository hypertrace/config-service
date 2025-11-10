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

  private static final String TENANT_ID_1 = "tenant-id-1";
  private static final String TENANT_ID_2 = "tenant-id-2";
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
    // case-1: rule has rule evaluation points other than PLATFORM
    CreateCustomSignatureRuleRequest createCustomSignatureRuleRequest1 =
        getCreateCustomSignatureRuleRequest1();
    CustomSignatureRule createdRule1 =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID_1,
                () ->
                    customSignatureConfigServiceBlockingStub.createCustomSignatureRule(
                        createCustomSignatureRuleRequest1))
            .getRule();
    String createdRuleId1 = createdRule1.getId();

    // before migration
    assertTrue(
        createdRule1
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM));
    assertTrue(
        createdRule1
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));
    assertTrue(
        createdRule1
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT));

    List<CustomSignatureRule> allFetchedRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID_1,
            () ->
                customSignatureConfigServiceBlockingStub
                    .getCustomSignatureRules(
                        GetCustomSignatureRulesRequest.newBuilder()
                            .setFilter(GetRulesFilter.newBuilder().addRuleIds(createdRuleId1))
                            .build())
                    .getRulesList());

    // after migration
    if (allFetchedRules.isEmpty()) {
      throw new IllegalStateException(
          "Custom Signature rule with ID " + createdRuleId1 + " not found");
    }
    CustomSignatureRule filteredCustomSignatureRule1 = allFetchedRules.get(0);

    assertEquals(Category.CATEGORY_CUSTOM_SIGNATURE, filteredCustomSignatureRule1.getCategory());
    assertTrue(
        filteredCustomSignatureRule1
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));
    assertTrue(
        filteredCustomSignatureRule1
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT));
    assertFalse(
        filteredCustomSignatureRule1
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM));

    // case-2: rule has no rule evaluation points other than PLATFORM
    CreateCustomSignatureRuleRequest createCustomSignatureRuleRequest2 =
        getCreateCustomSignatureRuleRequest2();
    CustomSignatureRule createdRule2 =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID_2,
                () ->
                    customSignatureConfigServiceBlockingStub.createCustomSignatureRule(
                        createCustomSignatureRuleRequest2))
            .getRule();
    String createdRuleId2 = createdRule2.getId();

    // before migration
    assertTrue(
        createdRule1
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM));

    allFetchedRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID_2,
            () ->
                customSignatureConfigServiceBlockingStub
                    .getCustomSignatureRules(
                        GetCustomSignatureRulesRequest.newBuilder()
                            .setFilter(GetRulesFilter.newBuilder().addRuleIds(createdRuleId2))
                            .build())
                    .getRulesList());

    // after migration
    if (allFetchedRules.isEmpty()) {
      throw new IllegalStateException(
          "Custom Signature rule with ID " + createdRuleId2 + " not found");
    }
    CustomSignatureRule filteredCustomSignatureRule2 = allFetchedRules.get(0);

    assertEquals(Category.CATEGORY_CUSTOM_SIGNATURE, filteredCustomSignatureRule2.getCategory());
    assertTrue(
        filteredCustomSignatureRule2
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT));
    assertFalse(
        filteredCustomSignatureRule2
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM));
  }

  private CreateCustomSignatureRuleRequest getCreateCustomSignatureRuleRequest1() {
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

  private CreateCustomSignatureRuleRequest getCreateCustomSignatureRuleRequest2() {
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
                                        .setMatchKey(MatchKey.MATCH_KEY_HEADER_NAME)
                                        .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                        .setValue(
                                            Value.newBuilder().setStringValue("header-str-val"))))))
        .setEffect(
            RuleEffect.newBuilder()
                .setEventType(EventType.EVENT_TYPE_ALLOW)
                .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
        .setRuleScope(
            RuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-id")))
        .setRuleSource(RuleSource.RULE_SOURCE_CUSTOMER)
        .build();
  }
}
