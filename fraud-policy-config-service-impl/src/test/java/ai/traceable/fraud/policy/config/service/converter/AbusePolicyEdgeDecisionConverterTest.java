package ai.traceable.fraud.policy.config.service.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.edge.decision.config.service.v1.PolicyKind;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigServiceGrpc.EntityDerivationConfigServiceBlockingStub;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.PrepopulatedSpanAttribute;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionType;
import ai.traceable.fraud.policy.config.service.v1.AbuseActionConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseActionType;
import ai.traceable.fraud.policy.config.service.v1.AbuseAggregationConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiIds;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseEnvironmentScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseGroupByConfig;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicy;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyData;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseRiskSeverity;
import ai.traceable.fraud.policy.config.service.v1.AbuseSimpleAggregationTemplateConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdOperator;
import ai.traceable.fraud.policy.config.service.v1.AbuseTimeWindow;
import com.google.protobuf.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AbusePolicyEdgeDecisionConverterTest {

  @Mock private EntityDerivationConfigServiceBlockingStub entityDerivationConfigServiceStub;
  @Mock private ApiScopeResolver apiScopeResolver;
  @Mock private PipelineToJexlConverter pipelineToJexlConverter;
  @Mock private CachedApiMappingProvider cachedApiMappingProvider;

  private AbusePolicyEdgeDecisionConverter converter;

  @BeforeEach
  void setUp() {
    when(pipelineToJexlConverter.apply(any(), any(), any())).thenAnswer(inv -> inv.getArgument(0));
    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), any())).thenReturn(Map.of());
    ScopeToJexlConverter scopeToJexlConverter = new ScopeToJexlConverter(cachedApiMappingProvider);
    EntityJexlResolver entityJexlResolver =
        new EntityJexlResolver(
            entityDerivationConfigServiceStub, pipelineToJexlConverter, scopeToJexlConverter);
    when(apiScopeResolver.resolveApiScope(any(), any())).thenReturn(Collections.emptyList());
    Map<AbusePolicyData.TemplateConfigCase, TemplateEdgeDecisionConverter> templateConverters =
        Map.of(
            AbusePolicyData.TemplateConfigCase.SIMPLE_AGGREGATION_TEMPLATE,
            new SimpleAggregationTemplateConverter(new DetectionFilterConverter()));
    converter =
        new AbusePolicyEdgeDecisionConverter(
            entityJexlResolver, apiScopeResolver, templateConverters);
  }

  @Test
  void convert_delegatesToTemplateConverter_producesCorrectRuleEnvelope() {
    EntityDerivationConfig ipConfig =
        EntityDerivationConfig.newBuilder()
            .setId("system_entity_ip")
            .setColumnName("ip_address")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setPrepopulatedSpanAttribute(PrepopulatedSpanAttribute.getDefaultInstance()))
            .build();

    when(entityDerivationConfigServiceStub.getEntityDerivationConfigs(any()))
        .thenReturn(
            GetEntityDerivationConfigsResponse.newBuilder()
                .addEntityDerivationConfigs(ipConfig)
                .build());

    AbusePolicy policy = buildSampleAbusePolicy();
    EdgeDecisionEngineConfig result = convert(List.of(policy));

    assertEquals(1, result.getDecisionRulesCount());
    EdgeDecisionRule rule = result.getDecisionRules(0);

    // Verify orchestration-level envelope fields
    assertEquals("3a522041-1761-4ce1-94d0-1fca377b444c", rule.getId());
    assertEquals("abc", rule.getName());
    assertEquals(
        EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_ABUSE_DETECTION,
        rule.getRuleCategory());
    assertEquals(
        EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK, rule.getRuleDecision().getEdgeDecisionType());
    assertEquals("abc", rule.getRuleDecision().getThreatType());
    assertEquals(PolicyKind.POLICY_KIND_BOT_MITIGATION, rule.getPolicyKind());
    assertEquals("3a522041-1761-4ce1-94d0-1fca377b444c", rule.getPolicyId());
    assertFalse(rule.getRuleStatus().getDisabled());

    assertTrue(rule.hasRuleScope());
    assertEquals(1, rule.getRuleScope().getScopeConditionsCount());
    assertEquals(
        "xyz-env",
        rule.getRuleScope().getScopeConditions(0).getEnvironmentScope().getEnvironments(0));
  }

  @Test
  void convert_skipsAlertActionPolicy() {
    when(entityDerivationConfigServiceStub.getEntityDerivationConfigs(any()))
        .thenReturn(GetEntityDerivationConfigsResponse.getDefaultInstance());

    AbusePolicy alertPolicy =
        AbusePolicy.newBuilder()
            .setId("alert-policy")
            .setData(
                AbusePolicyData.newBuilder()
                    .setName("Alert Policy")
                    .setEnabled(true)
                    .setAction(
                        AbuseActionConfig.newBuilder()
                            .setActionType(AbuseActionType.ABUSE_ACTION_TYPE_ALERT))
                    .setSimpleAggregationTemplate(
                        AbuseSimpleAggregationTemplateConfig.newBuilder()
                            .setAggregation(
                                AbuseAggregationConfig.newBuilder()
                                    .setAggregationFunction(
                                        AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT)
                                    .setDerivedEntityId("some_entity"))))
            .build();

    EdgeDecisionEngineConfig result = convert(List.of(alertPolicy));
    assertEquals(0, result.getDecisionRulesCount());
  }

  @Test
  void convert_skipsDisabledPolicy() {
    when(entityDerivationConfigServiceStub.getEntityDerivationConfigs(any()))
        .thenReturn(GetEntityDerivationConfigsResponse.getDefaultInstance());

    AbusePolicy disabledPolicy =
        AbusePolicy.newBuilder()
            .setId("disabled-policy")
            .setData(
                AbusePolicyData.newBuilder()
                    .setName("Disabled Policy")
                    .setEnabled(false)
                    .setAction(
                        AbuseActionConfig.newBuilder()
                            .setActionType(AbuseActionType.ABUSE_ACTION_TYPE_BLOCK))
                    .setSimpleAggregationTemplate(
                        AbuseSimpleAggregationTemplateConfig.newBuilder()
                            .setAggregation(
                                AbuseAggregationConfig.newBuilder()
                                    .setAggregationFunction(
                                        AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT)
                                    .setDerivedEntityId("some_entity"))))
            .build();

    EdgeDecisionEngineConfig result = convert(List.of(disabledPolicy));
    assertEquals(0, result.getDecisionRulesCount());
  }

  @Test
  void convert_skipsPredefinedTemplatePolicy() {
    when(entityDerivationConfigServiceStub.getEntityDerivationConfigs(any()))
        .thenReturn(GetEntityDerivationConfigsResponse.getDefaultInstance());

    AbusePolicy predefinedPolicy =
        AbusePolicy.newBuilder()
            .setId("predefined-policy")
            .setData(
                AbusePolicyData.newBuilder()
                    .setName("Predefined Policy")
                    .setEnabled(true)
                    .setAction(
                        AbuseActionConfig.newBuilder()
                            .setActionType(AbuseActionType.ABUSE_ACTION_TYPE_BLOCK)))
            .build();

    EdgeDecisionEngineConfig result = convert(List.of(predefinedPolicy));
    assertEquals(0, result.getDecisionRulesCount());
  }

  @Test
  void convert_multiplePolicies_convertsOnlyBlockEnabled() {
    EntityDerivationConfig ipConfig =
        EntityDerivationConfig.newBuilder()
            .setId("system_entity_ip")
            .setColumnName("ip_address")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setPrepopulatedSpanAttribute(PrepopulatedSpanAttribute.getDefaultInstance()))
            .build();

    when(entityDerivationConfigServiceStub.getEntityDerivationConfigs(any()))
        .thenReturn(
            GetEntityDerivationConfigsResponse.newBuilder()
                .addEntityDerivationConfigs(ipConfig)
                .build());

    AbusePolicy blockPolicy = buildBlockPolicy("system_entity_ip");
    AbusePolicy alertPolicy =
        AbusePolicy.newBuilder()
            .setId("alert-policy")
            .setData(
                AbusePolicyData.newBuilder()
                    .setName("Alert Only")
                    .setEnabled(true)
                    .setAction(
                        AbuseActionConfig.newBuilder()
                            .setActionType(AbuseActionType.ABUSE_ACTION_TYPE_ALERT))
                    .setSimpleAggregationTemplate(
                        AbuseSimpleAggregationTemplateConfig.newBuilder()
                            .setAggregation(
                                AbuseAggregationConfig.newBuilder()
                                    .setAggregationFunction(
                                        AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT)
                                    .setDerivedEntityId("system_entity_ip"))))
            .build();

    EdgeDecisionEngineConfig result = convert(List.of(blockPolicy, alertPolicy));
    assertEquals(1, result.getDecisionRulesCount());
    assertEquals("block-policy", result.getDecisionRules(0).getId());
  }

  // --- Helper methods ---

  private EdgeDecisionEngineConfig convert(List<AbusePolicy> policies) {
    RequestContext requestContext = RequestContext.forTenantId("test-tenant");
    return requestContext.call(() -> converter.convert(requestContext, policies));
  }

  private AbusePolicy buildSampleAbusePolicy() {
    return AbusePolicy.newBuilder()
        .setId("3a522041-1761-4ce1-94d0-1fca377b444c")
        .setData(
            AbusePolicyData.newBuilder()
                .setName("abc")
                .setEnabled(true)
                .setSeverity(AbuseRiskSeverity.ABUSE_RISK_SEVERITY_MEDIUM)
                .setMessageFormat("Policy alert: abc")
                .setScope(
                    AbusePolicyScope.newBuilder()
                        .setEnvironmentScope(
                            AbuseEnvironmentScope.newBuilder().addEnvironmentIds("xyz-env"))
                        .setApiScope(
                            AbuseApiScope.newBuilder()
                                .setApiIds(
                                    AbuseApiIds.newBuilder()
                                        .addIds("119c6ca2-f003-3b6b-9701-6f824cc3a78e"))))
                .setAction(
                    AbuseActionConfig.newBuilder()
                        .setActionType(AbuseActionType.ABUSE_ACTION_TYPE_BLOCK))
                .setSimpleAggregationTemplate(
                    AbuseSimpleAggregationTemplateConfig.newBuilder()
                        .setAggregation(
                            AbuseAggregationConfig.newBuilder()
                                .setAggregationFunction(
                                    AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT)
                                .setDerivedEntityId("system_entity_ip"))
                        .setThreshold(
                            AbuseThresholdConfig.newBuilder()
                                .setOperator(
                                    AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_GREATER_THAN)
                                .setValue(5))
                        .setTimeWindow(
                            AbuseTimeWindow.newBuilder()
                                .setLookbackDuration(Duration.newBuilder().setSeconds(300)))))
        .build();
  }

  private AbusePolicy buildBlockPolicy(String entityId) {
    return AbusePolicy.newBuilder()
        .setId("block-policy")
        .setData(
            AbusePolicyData.newBuilder()
                .setName("Block Policy")
                .setEnabled(true)
                .setSeverity(AbuseRiskSeverity.ABUSE_RISK_SEVERITY_MEDIUM)
                .setAction(
                    AbuseActionConfig.newBuilder()
                        .setActionType(AbuseActionType.ABUSE_ACTION_TYPE_BLOCK))
                .setMessageFormat("alert")
                .setSimpleAggregationTemplate(
                    AbuseSimpleAggregationTemplateConfig.newBuilder()
                        .setAggregation(
                            AbuseAggregationConfig.newBuilder()
                                .setAggregationFunction(
                                    AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT)
                                .setDerivedEntityId(entityId))
                        .setGroupBy(AbuseGroupByConfig.newBuilder().setDerivedEntityId(entityId))
                        .setThreshold(
                            AbuseThresholdConfig.newBuilder()
                                .setOperator(
                                    AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_GREATER_THAN)
                                .setValue(10))
                        .setTimeWindow(
                            AbuseTimeWindow.newBuilder()
                                .setLookbackDuration(Duration.newBuilder().setSeconds(300)))))
        .build();
  }
}
