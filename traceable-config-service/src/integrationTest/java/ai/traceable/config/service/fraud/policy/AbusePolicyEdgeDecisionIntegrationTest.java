package ai.traceable.config.service.fraud.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.v1.AggregateThresholdRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityCategory;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigServiceGrpc;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EnvironmentScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocation;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocationType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.Scope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanBasedExtraction;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanProjection;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionType;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorType;
import ai.traceable.fraud.policy.config.service.v1.AbuseActionConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseActionType;
import ai.traceable.fraud.policy.config.service.v1.AbuseAggregationConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiIds;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseEnvironmentScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseGroupByConfig;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicy;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyData;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyDetectionFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLiteralValues;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyRelationalFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseRiskSeverity;
import ai.traceable.fraud.policy.config.service.v1.AbuseSimpleAggregationTemplateConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdOperator;
import ai.traceable.fraud.policy.config.service.v1.AbuseTimeWindow;
import ai.traceable.fraud.policy.config.service.v1.CreateAbusePolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteAbusePolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyConfigServiceGrpc;
import ai.traceable.fraud.policy.config.service.v1.GetAbusePolicyEdgeDecisionRulesRequest;
import ai.traceable.fraud.policy.config.service.v1.GetAbusePolicyEdgeDecisionRulesResponse;
import com.google.protobuf.Duration;
import com.google.protobuf.Value;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Real-service integration test for the abuse policy -> EDS rule conversion pipeline. Validates
 * that entity derivation configs created via gRPC are correctly resolved into EDS rule_variables,
 * and that detection filters reference those variables by name in match_condition JEXL.
 */
public class AbusePolicyEdgeDecisionIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {

  private static final String TEST_TENANT = "tenant-abuse-eds-test";

  private static FraudPolicyConfigServiceGrpc.FraudPolicyConfigServiceBlockingStub policyStub;
  private static EntityDerivationConfigServiceGrpc.EntityDerivationConfigServiceBlockingStub
      entityStub;

  @BeforeAll
  static void init() {
    policyStub =
        FraudPolicyConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    entityStub =
        EntityDerivationConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void abusePolicyEdgeDecisionRules_producesCorrectVariablesAndFilterJexl() {
    // 1. Create a custom header entity for use as a detection filter
    CreateEntityDerivationConfigResponse entityResponse =
        RequestContext.forTenantId(TEST_TENANT)
            .call(
                () ->
                    entityStub.createEntityDerivationConfig(
                        CreateEntityDerivationConfigRequest.newBuilder()
                            .setData(
                                EntityDerivationConfigData.newBuilder()
                                    .setDisplayName("Custom Filter Header")
                                    .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
                                    .setEventKind(
                                        ComplexDataModelEventKind.newBuilder()
                                            .setKindId("system_event_kind_string"))
                                    .setSpanProjection(
                                        SpanProjection.newBuilder()
                                            .addEventDerivationConfigs(
                                                EventDerivationConfigDetails.newBuilder()
                                                    .setName("Header extraction")
                                                    .setScope(
                                                        Scope.newBuilder()
                                                            .setEnvironmentScope(
                                                                EnvironmentScope
                                                                    .getDefaultInstance()))
                                                    .setSpanExtraction(
                                                        SpanBasedExtraction.newBuilder()
                                                            .setLocation(
                                                                ExtractionLocation.newBuilder()
                                                                    .setLocationType(
                                                                        ExtractionLocationType
                                                                            .EXTRACTION_LOCATION_TYPE_REQUEST_HEADER)
                                                                    .setKey("x-custom-filter"))))))
                            .build()));

    EntityDerivationConfig customEntity = entityResponse.getEntityDerivationConfig();
    String customEntityId = customEntity.getId();
    assertEquals("custom_filter_header", customEntity.getColumnName());

    try {
      // 2. Create a BLOCK abuse policy: COUNT by ip_address, filter on the custom entity
      AbusePolicy policy =
          RequestContext.forTenantId(TEST_TENANT)
              .call(
                  () ->
                      policyStub
                          .createAbusePolicy(
                              CreateAbusePolicyRequest.newBuilder()
                                  .setData(
                                      AbusePolicyData.newBuilder()
                                          .setName("IT Filter Test Policy")
                                          .setEnabled(true)
                                          .setSeverity(AbuseRiskSeverity.ABUSE_RISK_SEVERITY_HIGH)
                                          .setMessageFormat("alert")
                                          .setScope(
                                              AbusePolicyScope.newBuilder()
                                                  .setEnvironmentScope(
                                                      AbuseEnvironmentScope.newBuilder()
                                                          .addEnvironmentIds(""))
                                                  .setApiScope(
                                                      AbuseApiScope.newBuilder()
                                                          .setApiIds(AbuseApiIds.newBuilder())))
                                          .setAction(
                                              AbuseActionConfig.newBuilder()
                                                  .setActionType(
                                                      AbuseActionType.ABUSE_ACTION_TYPE_BLOCK))
                                          .setSimpleAggregationTemplate(
                                              AbuseSimpleAggregationTemplateConfig.newBuilder()
                                                  .setAggregation(
                                                      AbuseAggregationConfig.newBuilder()
                                                          .setAggregationFunction(
                                                              AggregationFunctionType
                                                                  .AGGREGATION_FUNCTION_TYPE_COUNT)
                                                          .setDerivedEntityId(customEntityId))
                                                  .setGroupBy(
                                                      AbuseGroupByConfig.newBuilder()
                                                          .setDerivedEntityId(
                                                              "system_entity_ip_address"))
                                                  .setThreshold(
                                                      AbuseThresholdConfig.newBuilder()
                                                          .setOperator(
                                                              AbuseThresholdOperator
                                                                  .ABUSE_THRESHOLD_OPERATOR_GREATER_THAN)
                                                          .setValue(5))
                                                  .setTimeWindow(
                                                      AbuseTimeWindow.newBuilder()
                                                          .setLookbackDuration(
                                                              Duration.newBuilder().setSeconds(60)))
                                                  .addFilters(
                                                      AbusePolicyDetectionFilter.newBuilder()
                                                          .setRelationalFilter(
                                                              AbusePolicyRelationalFilter
                                                                  .newBuilder()
                                                                  .setDerivedEntityId(
                                                                      customEntityId)
                                                                  .setOperator(
                                                                      OperatorType
                                                                          .OPERATOR_TYPE_STRING_EQUALS)
                                                                  .setLiteralValues(
                                                                      AbusePolicyLiteralValues
                                                                          .newBuilder()
                                                                          .addValues(
                                                                              Value.newBuilder()
                                                                                  .setStringValue(
                                                                                      "expected_value")))))))
                                  .build())
                          .getPolicy());

      try {
        // 3. Call getAbusePolicyEdgeDecisionRules
        GetAbusePolicyEdgeDecisionRulesResponse edsResponse =
            RequestContext.forTenantId(TEST_TENANT)
                .call(
                    () ->
                        policyStub.getAbusePolicyEdgeDecisionRules(
                            GetAbusePolicyEdgeDecisionRulesRequest.getDefaultInstance()));

        EdgeDecisionEngineConfig engineConfig = edsResponse.getEdgeDecisionEngineConfig();
        assertEquals(1, engineConfig.getDecisionRulesCount());

        // 4. Assert the full EDS rule output
        EdgeDecisionRule rule = engineConfig.getDecisionRules(0);
        assertEquals(policy.getId(), rule.getId());
        assertEquals("IT Filter Test Policy", rule.getName());
        assertEquals(
            EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_ABUSE_DETECTION,
            rule.getRuleCategory());
        assertEquals(
            EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK,
            rule.getRuleDecision().getEdgeDecisionType());
        assertFalse(rule.getRuleStatus().getDisabled());

        EdgeDecisionRuleDefinition def = rule.getRuleDefinition();

        // rule_variables: ip_address + custom_filter_header
        assertEquals(2, def.getRuleVariablesCount());
        VariableDerivationMapping ipVar = findVariable(def, "ip_address");
        VariableDerivationMapping filterVar = findVariable(def, "custom_filter_header");

        assertEquals(1, ipVar.getRulesCount());
        assertEquals(
            "$s.getIpAddress()",
            ipVar.getRules(0).getTransformationConfig().getJexlExpression().getJexlExpression());

        assertEquals(1, filterVar.getRulesCount());
        assertEquals(
            "$s.getRequestHeaders().get('x-custom-filter')",
            filterVar
                .getRules(0)
                .getTransformationConfig()
                .getJexlExpression()
                .getJexlExpression());

        // aggregate_threshold_rule
        AggregateThresholdRule aggRule = def.getAggregateThresholdRule();

        // match_condition references the filter variable by name
        assertTrue(aggRule.hasMatchCondition());
        assertEquals(
            "custom_filter_header.equals('expected_value')",
            aggRule
                .getMatchCondition()
                .getGenericMatchCondition()
                .getJexlExpression()
                .getJexlExpression());

        // group_by references ip_address variable
        assertEquals(1, aggRule.getGroupByDimensionsCount());
        assertEquals("ip_address", aggRule.getGroupByDimensions(0).getName());

        // threshold and time window
        assertEquals(5.0, aggRule.getValueAggregateThreshold().getStaticThreshold());
        assertEquals(
            ValueAggregateThreshold.ThresholdOperator.THRESHOLD_OPERATOR_ABOVE,
            aggRule.getValueAggregateThreshold().getThresholdOperator());
        assertEquals(60, aggRule.getTimeWindow().getSeconds());

      } finally {
        // Cleanup: delete policy
        RequestContext.forTenantId(TEST_TENANT)
            .call(
                () ->
                    policyStub.deleteAbusePolicy(
                        DeleteAbusePolicyRequest.newBuilder()
                            .addPolicyIds(policy.getId())
                            .build()));
      }
    } finally {
      // Cleanup: delete custom entity
      RequestContext.forTenantId(TEST_TENANT)
          .call(
              () ->
                  entityStub.deleteEntityDerivationConfig(
                      DeleteEntityDerivationConfigRequest.newBuilder()
                          .setEntityDerivationConfigId(customEntityId)
                          .build()));
    }
  }

  private static VariableDerivationMapping findVariable(
      EdgeDecisionRuleDefinition def, String name) {
    return def.getRuleVariablesList().stream()
        .filter(v -> v.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Variable not found: " + name));
  }
}
