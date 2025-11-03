package ai.traceable.config.service.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.customsignature.config.service.CustomSignatureConfigServiceConfig;
import ai.traceable.customsignature.config.service.rules.CustomSignatureRuleConverter;
import ai.traceable.customsignature.config.service.rules.CustomSignatureRulesStore;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
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
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.customsignature.config.service.v1.RuleSource;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import com.google.protobuf.Value;
import com.typesafe.config.ConfigFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CustomSignatureRuleEvaluationPointsMigrationConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub
      customSignatureConfigServiceBlockingStub;
  private static CustomSignatureRulesStore customSignatureRulesStore;

  private static final String TENANT_ID = "tenant1";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final String APPLICATION_CONFIG =
      "configs/traceable-config-service/application.conf";

  @BeforeAll
  static void init() {
    customSignatureConfigServiceBlockingStub =
        CustomSignatureConfigServiceGrpc.newBlockingStub(channelForInternalServices)
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

    CustomSignatureConfigServiceConfig customSignatureConfigServiceConfig =
        new CustomSignatureConfigServiceConfig(
            ConfigFactory.parseURL(
                Objects.requireNonNull(
                    CustomSignatureRuleEvaluationPointsMigrationConfigServiceIntegrationTest.class
                        .getClassLoader()
                        .getResource(APPLICATION_CONFIG))));

    customSignatureRulesStore =
        new CustomSignatureRulesStore(
            configServiceBlockingStub,
            new CustomSignatureRuleConverter(),
            configChangeEventGenerator,
            customSignatureConfigServiceConfig);
  }

  @Test
  void testMigrationForRuleEvaluationPoints() {

    RuleDefinition defaultRuleDefinition =
        RuleDefinition.newBuilder()
            .setClauseGroup(
                ClauseGroup.newBuilder()
                    .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                    .addClauses(
                        Clause.newBuilder()
                            .setMatchExpression(
                                MatchExpression.newBuilder()
                                    .setMatchKey(MatchKey.MATCH_KEY_HOST)
                                    .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                    .setMatchValue("127.0.0.1")))
                    .addClauses(
                        Clause.newBuilder()
                            .setKeyValueExpression(
                                KeyValueExpression.newBuilder()
                                    .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                    .setKeyMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                    .setMatchKey("x-real-ip")
                                    .setValueMatchOperator(
                                        MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                    .setMatchValue("^127"))))
            .build();

    RuleEffect ruleEffect1 =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
            .build();

    RuleEffect ruleEffect2 =
        RuleEffect.newBuilder()
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .build();

    /*
     * creating a rule without RuleEvaluationPoints - expectation is that migration should happen
     */
    CustomSignatureRule createdRule =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                customSignatureConfigServiceBlockingStub
                    .createCustomSignatureRule(
                        CreateCustomSignatureRuleRequest.newBuilder()
                            .setName("rule-without-any-evaluation-points")
                            .setDefinition(defaultRuleDefinition)
                            .setEffect(ruleEffect1)
                            .setRuleSource(RuleSource.RULE_SOURCE_CUSTOMER)
                            .build())
                    .getRule());
    assertTrue(
        createdRule
            .getEffect()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM));

    /*
     * Updating a rule without RuleEvaluationPoints - expectation is that migration should happen
     */
    CustomSignatureRule ruleToUpdate =
        CustomSignatureRule.newBuilder()
            .setName("updated-rule-name")
            .setDefinition(defaultRuleDefinition)
            .setEffect(ruleEffect2)
            .setId(createdRule.getId())
            .build();
    CustomSignatureRule updatedRule =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    customSignatureConfigServiceBlockingStub.updateCustomSignatureRule(
                        UpdateCustomSignatureRuleRequest.newBuilder()
                            .setRule(ruleToUpdate)
                            .build()))
            .getRule();
    assertTrue(
        updatedRule
            .getEffect()
            .getRuleEvaluationPointsList()
            .containsAll(
                Arrays.asList(
                    RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM,
                    RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)));

    /*
     * Workflow for getCustomSignatureRules - this will migrate all rules without RuleEvaluationPoints
     */
    upsertCustomSignatureRuleWithoutRuleEvaluationPoints(
        defaultRuleDefinition,
        ruleEffect1,
        RuleScope.newBuilder()
            .setEnvironmentScope(
                ai.traceable.customsignature.config.service.v1.EnvironmentScope.newBuilder()
                    .addEnvironmentIds("env-id"))
            .build());

    List<CustomSignatureRule> fetchedRules = fetchAllCustomSignatureRules();

    assertEquals(2, fetchedRules.size());

    /*
     * Now, its expected that both the fetched rules will have a non-empty list of RuleEvaluationPoints
     */
    fetchedRules.forEach(
        customSignatureRule ->
            assertFalse(customSignatureRule.getEffect().getRuleEvaluationPointsList().isEmpty()));
  }

  private List<CustomSignatureRule> fetchAllCustomSignatureRules() {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            customSignatureConfigServiceBlockingStub
                .getCustomSignatureRules(
                    GetCustomSignatureRulesRequest.newBuilder()
                        .setFilter(
                            GetRulesFilter.newBuilder()
                                .addRuleSources(RuleSource.RULE_SOURCE_CUSTOMER))
                        .build())
                .getRulesList());
  }

  private void upsertCustomSignatureRuleWithoutRuleEvaluationPoints(
      RuleDefinition ruleDefinition, RuleEffect ruleEffect, RuleScope ruleScope) {
    String customSignatureRuleId = UUID.randomUUID().toString();
    CustomSignatureRule customSignatureRule =
        CustomSignatureRule.newBuilder()
            .setId(customSignatureRuleId)
            .setName("custom-signature-rule-with-no-rule-evaluation-points")
            .setDefinition(ruleDefinition)
            .setEffect(ruleEffect)
            .setRuleScope(ruleScope)
            .setRuleSource(RuleSource.RULE_SOURCE_CUSTOMER)
            .build();

    customSignatureRulesStore.upsertObjects(REQUEST_CONTEXT, List.of(customSignatureRule));
  }
}
