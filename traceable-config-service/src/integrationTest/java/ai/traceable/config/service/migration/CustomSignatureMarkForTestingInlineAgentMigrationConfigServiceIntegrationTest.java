package ai.traceable.config.service.migration;

import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.customsignature.config.service.v1.AgentModification;
import ai.traceable.customsignature.config.service.v1.AgentRuleEffect;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.FieldValue;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.HeaderInjection;
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
import com.google.protobuf.Value;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CustomSignatureMarkForTestingInlineAgentMigrationConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub
      customSignatureConfigServiceBlockingStub;

  private static final String TENANT_ID = "tenant-id";

  @BeforeAll
  static void init() {
    customSignatureConfigServiceBlockingStub =
        CustomSignatureConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testMigrationForMarkForTestingRulesToInlineTracingAgent() {
    CreateCustomSignatureRuleRequest createCustomSignatureRuleRequest =
        getCreateCustomSignatureRuleRequest();

    // rule hasn't been created with any rule evaluation points
    CustomSignatureRule createdRule =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    customSignatureConfigServiceBlockingStub.createCustomSignatureRule(
                        createCustomSignatureRuleRequest))
            .getRule();
    String createdRuleId = createdRule.getId();

    CustomSignatureRule fetchedRule =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    customSignatureConfigServiceBlockingStub
                        .getCustomSignatureRules(
                            GetCustomSignatureRulesRequest.newBuilder()
                                .setFilter(GetRulesFilter.newBuilder().addRuleIds(createdRuleId))
                                .build())
                        .getRulesList())
            .get(0);

    assertTrue(
        fetchedRule
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT));
  }

  private CreateCustomSignatureRuleRequest getCreateCustomSignatureRuleRequest() {
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
                .setEventType(EventType.EVENT_TYPE_TESTING_DETECTION)
                .addEffects(
                    RuleEffectWithModifications.newBuilder()
                        .setAgentRuleEffect(
                            AgentRuleEffect.newBuilder()
                                .addAgentModifications(
                                    AgentModification.newBuilder()
                                        .setHeaderInjection(
                                            HeaderInjection.newBuilder()
                                                .setHeaderName("header-injection-name")
                                                .setHeaderCategory(
                                                    MatchCategory.MATCH_CATEGORY_REQUEST)
                                                .setValue(
                                                    FieldValue.newBuilder()
                                                        .setStaticValue(
                                                            "header-injection-static-val")))))))
        .setRuleScope(
            RuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-id")))
        .setRuleSource(RuleSource.RULE_SOURCE_CUSTOMER)
        .build();
  }
}
