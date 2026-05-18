package ai.traceable.fraud.policy.config.service.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.edge.decision.config.service.v1.AggregateThresholdRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeInputKind;
import ai.traceable.edge.decision.config.service.v1.SpanAttributeDecoration;
import ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionType;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorType;
import ai.traceable.fraud.policy.config.service.v1.AbuseActionConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseActionType;
import ai.traceable.fraud.policy.config.service.v1.AbuseAggregationConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseGroupByConfig;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyData;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyDetectionFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLiteralValues;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLogicalFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLogicalOperator;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyRelationalFilter;
import ai.traceable.fraud.policy.config.service.v1.AbuseRiskSeverity;
import ai.traceable.fraud.policy.config.service.v1.AbuseSimpleAggregationTemplateConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdOperator;
import ai.traceable.fraud.policy.config.service.v1.AbuseTimeWindow;
import com.google.protobuf.Duration;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimpleAggregationTemplateConverterTest {

  private SimpleAggregationTemplateConverter converter;

  @BeforeEach
  void setUp() {
    converter = new SimpleAggregationTemplateConverter(new DetectionFilterConverter());
  }

  private static DerivationRule dummyRule(String jexl) {
    return DerivationRule.newBuilder()
        .setTransformationConfig(
            DataTransformationConfig.newBuilder()
                .setJexlExpression(JexlExpressionConfig.newBuilder().setJexlExpression(jexl)))
        .build();
  }

  private static Map<String, List<DerivationRule>> rulesMap(String... entityIdAndJexlPairs) {
    var builder = new java.util.HashMap<String, List<DerivationRule>>();
    for (int i = 0; i < entityIdAndJexlPairs.length; i += 2) {
      builder.put(entityIdAndJexlPairs[i], List.of(dummyRule(entityIdAndJexlPairs[i + 1])));
    }
    return builder;
  }

  // --- getRequiredDerivedEntityIds ---

  @Test
  void getRequiredDerivedEntityIds_collectsGroupByAndAggregationIds() {
    AbusePolicyData data =
        AbusePolicyData.newBuilder()
            .setSimpleAggregationTemplate(
                AbuseSimpleAggregationTemplateConfig.newBuilder()
                    .setAggregation(
                        AbuseAggregationConfig.newBuilder()
                            .setDerivedEntityId("agg_entity")
                            .setAggregationFunction(
                                AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT))
                    .setGroupBy(
                        AbuseGroupByConfig.newBuilder().setDerivedEntityId("groupby_entity")))
            .build();

    Set<String> ids = converter.getRequiredDerivedEntityIds(data);

    assertEquals(Set.of("agg_entity", "groupby_entity"), ids);
  }

  @Test
  void getRequiredDerivedEntityIds_collectsFilterEntityIds() {
    AbusePolicyData data =
        AbusePolicyData.newBuilder()
            .setSimpleAggregationTemplate(
                AbuseSimpleAggregationTemplateConfig.newBuilder()
                    .setAggregation(
                        AbuseAggregationConfig.newBuilder()
                            .setDerivedEntityId("agg_entity")
                            .setAggregationFunction(
                                AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT))
                    .addFilters(
                        AbusePolicyDetectionFilter.newBuilder()
                            .setRelationalFilter(
                                AbusePolicyRelationalFilter.newBuilder()
                                    .setDerivedEntityId("filter_entity"))))
            .build();

    Set<String> ids = converter.getRequiredDerivedEntityIds(data);

    assertTrue(ids.contains("filter_entity"));
    assertTrue(ids.contains("agg_entity"));
  }

  @Test
  void getRequiredDerivedEntityIds_collectsNestedLogicalFilterEntityIds() {
    AbusePolicyData data =
        AbusePolicyData.newBuilder()
            .setSimpleAggregationTemplate(
                AbuseSimpleAggregationTemplateConfig.newBuilder()
                    .setAggregation(
                        AbuseAggregationConfig.newBuilder()
                            .setDerivedEntityId("agg_entity")
                            .setAggregationFunction(
                                AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT))
                    .addFilters(
                        AbusePolicyDetectionFilter.newBuilder()
                            .setLogicalFilter(
                                AbusePolicyLogicalFilter.newBuilder()
                                    .setOperator(
                                        AbusePolicyLogicalOperator.ABUSE_POLICY_LOGICAL_OPERATOR_OR)
                                    .addOperands(
                                        AbusePolicyDetectionFilter.newBuilder()
                                            .setRelationalFilter(
                                                AbusePolicyRelationalFilter.newBuilder()
                                                    .setDerivedEntityId("entity_a")))
                                    .addOperands(
                                        AbusePolicyDetectionFilter.newBuilder()
                                            .setRelationalFilter(
                                                AbusePolicyRelationalFilter.newBuilder()
                                                    .setDerivedEntityId("entity_b"))))))
            .build();

    Set<String> ids = converter.getRequiredDerivedEntityIds(data);

    assertEquals(Set.of("agg_entity", "entity_a", "entity_b"), ids);
  }

  // --- buildRuleDefinition ---

  @Test
  void buildRuleDefinition_setsEdgeInputKind() {
    AbusePolicyData data = buildSimplePolicyData("entity_ip", "entity_ip");
    Map<String, List<DerivationRule>> entityRulesMap = rulesMap("entity_ip", "$s.getIpAddress()");

    EdgeDecisionRuleDefinition def = converter.buildRuleDefinition(data, entityRulesMap, Map.of());

    assertEquals(EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST, def.getEdgeInputKind());
  }

  @Test
  void buildRuleDefinition_populatesRuleVariables() {
    AbusePolicyData data = buildSimplePolicyData("entity_ip", "entity_ip");
    Map<String, List<DerivationRule>> entityRulesMap = rulesMap("entity_ip", "$s.getIpAddress()");

    EdgeDecisionRuleDefinition def = converter.buildRuleDefinition(data, entityRulesMap, Map.of());

    assertEquals(1, def.getRuleVariablesCount());
    assertEquals("entity_ip", def.getRuleVariables(0).getName());
    assertEquals(1, def.getRuleVariables(0).getRulesCount());
    assertEquals(
        "$s.getIpAddress()",
        def.getRuleVariables(0)
            .getRules(0)
            .getTransformationConfig()
            .getJexlExpression()
            .getJexlExpression());
  }

  @Test
  void buildRuleDefinition_groupByDimension_referencesVariableName() {
    AbusePolicyData data = buildSimplePolicyData("entity_ip", "entity_ip");
    Map<String, List<DerivationRule>> entityRulesMap = rulesMap("entity_ip", "$s.getIpAddress()");

    EdgeDecisionRuleDefinition def = converter.buildRuleDefinition(data, entityRulesMap, Map.of());
    AggregateThresholdRule aggRule = def.getAggregateThresholdRule();

    assertEquals(1, aggRule.getGroupByDimensionsCount());
    // group_by references the variable name, not the inline JEXL
    assertEquals(
        "entity_ip",
        aggRule
            .getGroupByDimensions(0)
            .getRules(0)
            .getTransformationConfig()
            .getJexlExpression()
            .getJexlExpression());
  }

  @Test
  void buildRuleDefinition_groupByEntityNoRules_throwsException() {
    AbusePolicyData data = buildSimplePolicyData("entity_ip", "entity_missing");
    Map<String, List<DerivationRule>> entityRulesMap = rulesMap("entity_ip", "$s.getIpAddress()");

    assertThrows(
        IllegalStateException.class,
        () -> converter.buildRuleDefinition(data, entityRulesMap, Map.of()));
  }

  @Test
  void buildRuleDefinition_noGroupBy_emptyGroupByDimensions() {
    AbusePolicyData data =
        AbusePolicyData.newBuilder()
            .setName("no-groupby")
            .setSimpleAggregationTemplate(
                AbuseSimpleAggregationTemplateConfig.newBuilder()
                    .setAggregation(
                        AbuseAggregationConfig.newBuilder()
                            .setAggregationFunction(
                                AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT)
                            .setDerivedEntityId("entity_ip"))
                    .setThreshold(
                        AbuseThresholdConfig.newBuilder()
                            .setOperator(
                                AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_GREATER_THAN)
                            .setValue(5))
                    .setTimeWindow(
                        AbuseTimeWindow.newBuilder()
                            .setLookbackDuration(Duration.newBuilder().setSeconds(60))))
            .build();
    Map<String, List<DerivationRule>> entityRulesMap = rulesMap("entity_ip", "$s.getIpAddress()");

    AggregateThresholdRule aggRule =
        converter.buildRuleDefinition(data, entityRulesMap, Map.of()).getAggregateThresholdRule();

    assertEquals(0, aggRule.getGroupByDimensionsCount());
  }

  @Test
  void buildRuleDefinition_distinctCountSetsAggregationDimension() {
    AbusePolicyData data =
        AbusePolicyData.newBuilder()
            .setName("distinct-count")
            .setSimpleAggregationTemplate(
                AbuseSimpleAggregationTemplateConfig.newBuilder()
                    .setAggregation(
                        AbuseAggregationConfig.newBuilder()
                            .setAggregationFunction(
                                AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_DISTINCT_COUNT)
                            .setDerivedEntityId("entity_asn"))
                    .setThreshold(
                        AbuseThresholdConfig.newBuilder()
                            .setOperator(
                                AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_GREATER_THAN)
                            .setValue(5))
                    .setTimeWindow(
                        AbuseTimeWindow.newBuilder()
                            .setLookbackDuration(Duration.newBuilder().setSeconds(300))))
            .build();
    Map<String, List<DerivationRule>> entityRulesMap = rulesMap("entity_asn", "$s.getIpAsn()");

    ValueAggregateThreshold vat =
        converter
            .buildRuleDefinition(data, entityRulesMap, Map.of())
            .getAggregateThresholdRule()
            .getValueAggregateThreshold();

    assertEquals(
        ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_DISTINCT_COUNT,
        vat.getAggregationType());
    assertTrue(vat.hasDimension());
    // dimension references variable name
    assertEquals(
        "entity_asn",
        vat.getDimension()
            .getRules(0)
            .getTransformationConfig()
            .getJexlExpression()
            .getJexlExpression());
    assertEquals(5.0, vat.getStaticThreshold());
  }

  @Test
  void buildRuleDefinition_countSetsDimensionWhenEntityAvailable() {
    AbusePolicyData data = buildSimplePolicyData("entity_ip", "entity_ip");
    Map<String, List<DerivationRule>> entityRulesMap = rulesMap("entity_ip", "$s.getIpAddress()");

    ValueAggregateThreshold vat =
        converter
            .buildRuleDefinition(data, entityRulesMap, Map.of())
            .getAggregateThresholdRule()
            .getValueAggregateThreshold();

    assertEquals(
        ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_COUNT, vat.getAggregationType());
    assertTrue(vat.hasDimension());
    assertEquals(
        "entity_ip",
        vat.getDimension()
            .getRules(0)
            .getTransformationConfig()
            .getJexlExpression()
            .getJexlExpression());
  }

  @Test
  void buildRuleDefinition_timeWindow() {
    AbusePolicyData data = buildSimplePolicyData("entity_ip", "entity_ip");
    Map<String, List<DerivationRule>> entityRulesMap = rulesMap("entity_ip", "$s.getIpAddress()");

    AggregateThresholdRule aggRule =
        converter.buildRuleDefinition(data, entityRulesMap, Map.of()).getAggregateThresholdRule();

    assertEquals(300, aggRule.getTimeWindow().getSeconds());
  }

  @Test
  void buildRuleDefinition_thresholdOperatorAbove() {
    AbusePolicyData data = buildSimplePolicyData("entity_ip", "entity_ip");
    Map<String, List<DerivationRule>> entityRulesMap = rulesMap("entity_ip", "$s.getIpAddress()");

    ValueAggregateThreshold vat =
        converter
            .buildRuleDefinition(data, entityRulesMap, Map.of())
            .getAggregateThresholdRule()
            .getValueAggregateThreshold();

    assertEquals(
        ValueAggregateThreshold.ThresholdOperator.THRESHOLD_OPERATOR_ABOVE,
        vat.getThresholdOperator());
    assertEquals(10.0, vat.getStaticThreshold());
  }

  @Test
  void buildRuleDefinition_thresholdOperatorBelow() {
    AbusePolicyData data =
        AbusePolicyData.newBuilder()
            .setName("below-test")
            .setSimpleAggregationTemplate(
                AbuseSimpleAggregationTemplateConfig.newBuilder()
                    .setAggregation(
                        AbuseAggregationConfig.newBuilder()
                            .setAggregationFunction(
                                AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT)
                            .setDerivedEntityId("entity_ip"))
                    .setGroupBy(AbuseGroupByConfig.newBuilder().setDerivedEntityId("entity_ip"))
                    .setThreshold(
                        AbuseThresholdConfig.newBuilder()
                            .setOperator(AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_LESS_THAN)
                            .setValue(3))
                    .setTimeWindow(
                        AbuseTimeWindow.newBuilder()
                            .setLookbackDuration(Duration.newBuilder().setSeconds(60))))
            .build();
    Map<String, List<DerivationRule>> entityRulesMap = rulesMap("entity_ip", "$s.getIpAddress()");

    ValueAggregateThreshold vat =
        converter
            .buildRuleDefinition(data, entityRulesMap, Map.of())
            .getAggregateThresholdRule()
            .getValueAggregateThreshold();

    assertEquals(
        ValueAggregateThreshold.ThresholdOperator.THRESHOLD_OPERATOR_BELOW,
        vat.getThresholdOperator());
    assertEquals(3.0, vat.getStaticThreshold());
  }

  @Test
  void buildRuleDefinition_withRelationalFilter_buildsMatchCondition() {
    AbusePolicyData data =
        AbusePolicyData.newBuilder()
            .setName("filter-test")
            .setSimpleAggregationTemplate(
                AbuseSimpleAggregationTemplateConfig.newBuilder()
                    .setAggregation(
                        AbuseAggregationConfig.newBuilder()
                            .setAggregationFunction(
                                AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT)
                            .setDerivedEntityId("count_entity"))
                    .setGroupBy(AbuseGroupByConfig.newBuilder().setDerivedEntityId("count_entity"))
                    .setThreshold(
                        AbuseThresholdConfig.newBuilder()
                            .setOperator(
                                AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_GREATER_THAN)
                            .setValue(10))
                    .setTimeWindow(
                        AbuseTimeWindow.newBuilder()
                            .setLookbackDuration(Duration.newBuilder().setSeconds(60)))
                    .addFilters(
                        AbusePolicyDetectionFilter.newBuilder()
                            .setRelationalFilter(
                                AbusePolicyRelationalFilter.newBuilder()
                                    .setDerivedEntityId("header_entity")
                                    .setOperator(OperatorType.OPERATOR_TYPE_STRING_EQUALS)
                                    .setLiteralValues(
                                        AbusePolicyLiteralValues.newBuilder()
                                            .addValues(
                                                Value.newBuilder().setStringValue("test-value"))))))
            .build();
    Map<String, List<DerivationRule>> entityRulesMap =
        rulesMap(
            "count_entity", "$s.getIpAddress()",
            "header_entity", "$s.getRequestHeaders().get('x-custom')");

    AggregateThresholdRule aggRule =
        converter.buildRuleDefinition(data, entityRulesMap, Map.of()).getAggregateThresholdRule();

    assertTrue(aggRule.hasMatchCondition());
    MatchCondition mc = aggRule.getMatchCondition();
    assertTrue(mc.hasGenericMatchCondition());
    assertEquals(
        "header_entity == 'test-value'",
        mc.getGenericMatchCondition().getJexlExpression().getJexlExpression());
  }

  @Test
  void buildRuleDefinition_withLogicalFilter_buildsLogicalMatchCondition() {
    AbusePolicyDetectionFilter orFilter =
        AbusePolicyDetectionFilter.newBuilder()
            .setLogicalFilter(
                AbusePolicyLogicalFilter.newBuilder()
                    .setOperator(AbusePolicyLogicalOperator.ABUSE_POLICY_LOGICAL_OPERATOR_OR)
                    .addOperands(
                        AbusePolicyDetectionFilter.newBuilder()
                            .setRelationalFilter(
                                AbusePolicyRelationalFilter.newBuilder()
                                    .setDerivedEntityId("entity_a")
                                    .setOperator(OperatorType.OPERATOR_TYPE_STRING_EQUALS)
                                    .setLiteralValues(
                                        AbusePolicyLiteralValues.newBuilder()
                                            .addValues(
                                                Value.newBuilder().setStringValue("1.2.3.4")))))
                    .addOperands(
                        AbusePolicyDetectionFilter.newBuilder()
                            .setRelationalFilter(
                                AbusePolicyRelationalFilter.newBuilder()
                                    .setDerivedEntityId("entity_b")
                                    .setOperator(OperatorType.OPERATOR_TYPE_CONTAINS)
                                    .setLiteralValues(
                                        AbusePolicyLiteralValues.newBuilder()
                                            .addValues(Value.newBuilder().setStringValue("bot"))))))
            .build();

    AbusePolicyData data =
        AbusePolicyData.newBuilder()
            .setName("logical-filter-test")
            .setSimpleAggregationTemplate(
                AbuseSimpleAggregationTemplateConfig.newBuilder()
                    .setAggregation(
                        AbuseAggregationConfig.newBuilder()
                            .setAggregationFunction(
                                AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT)
                            .setDerivedEntityId("count_entity"))
                    .setThreshold(
                        AbuseThresholdConfig.newBuilder()
                            .setOperator(
                                AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_GREATER_THAN)
                            .setValue(10))
                    .setTimeWindow(
                        AbuseTimeWindow.newBuilder()
                            .setLookbackDuration(Duration.newBuilder().setSeconds(60)))
                    .addFilters(orFilter))
            .build();
    Map<String, List<DerivationRule>> entityRulesMap =
        rulesMap(
            "count_entity", "$s.getIpAddress()",
            "entity_a", "$s.getIpAddress()",
            "entity_b", "$s.getUserAgent()");

    AggregateThresholdRule aggRule =
        converter.buildRuleDefinition(data, entityRulesMap, Map.of()).getAggregateThresholdRule();

    assertTrue(aggRule.hasMatchCondition());
    MatchCondition mc = aggRule.getMatchCondition();
    assertTrue(mc.hasLogicalMatchCondition());
    assertEquals(
        LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR,
        mc.getLogicalMatchCondition().getOperator());
    assertEquals(2, mc.getLogicalMatchCondition().getConditionsCount());
    assertEquals(
        "entity_a == '1.2.3.4'",
        mc.getLogicalMatchCondition()
            .getConditions(0)
            .getGenericMatchCondition()
            .getJexlExpression()
            .getJexlExpression());
    assertEquals(
        "entity_b.contains('bot')",
        mc.getLogicalMatchCondition()
            .getConditions(1)
            .getGenericMatchCondition()
            .getJexlExpression()
            .getJexlExpression());
  }

  @Test
  void buildRuleDefinition_noFilters_noMatchCondition() {
    AbusePolicyData data = buildSimplePolicyData("entity_ip", "entity_ip");
    Map<String, List<DerivationRule>> entityRulesMap = rulesMap("entity_ip", "$s.getIpAddress()");

    AggregateThresholdRule aggRule =
        converter.buildRuleDefinition(data, entityRulesMap, Map.of()).getAggregateThresholdRule();

    assertFalse(aggRule.hasMatchCondition());
  }

  // --- buildSpanAttributes ---

  @Test
  void buildSpanAttributes_includesAllExpectedAttributes() {
    AbusePolicyData data = buildSimplePolicyData("entity_ip", "entity_ip");

    List<SpanAttributeDecoration> attrs = converter.buildSpanAttributes(data, Map.of());

    assertEquals(5, attrs.size());

    // actor
    assertEquals("actor", attrs.get(0).getSpanAttributeKey().getStaticValue().getStringValue());
    assertEquals(
        "$s.getIpAddress()",
        attrs.get(0).getSpanAttributeValue().getJexlExpression().getJexlExpression());

    // actor_type
    assertEquals(
        "actor_type", attrs.get(1).getSpanAttributeKey().getStaticValue().getStringValue());
    assertEquals(
        "IP ADDRESS", attrs.get(1).getSpanAttributeValue().getStaticValue().getStringValue());

    // account
    assertEquals("account", attrs.get(2).getSpanAttributeKey().getStaticValue().getStringValue());
    assertEquals(
        "$context.get('enduser.id')",
        attrs.get(2).getSpanAttributeValue().getJexlExpression().getJexlExpression());

    // account_type
    assertEquals(
        "account_type", attrs.get(3).getSpanAttributeKey().getStaticValue().getStringValue());
    assertEquals("USER ID", attrs.get(3).getSpanAttributeValue().getStaticValue().getStringValue());

    // detection_signature
    assertEquals(
        "detection_signature",
        attrs.get(4).getSpanAttributeKey().getStaticValue().getStringValue());
    assertTrue(
        attrs
            .get(4)
            .getSpanAttributeValue()
            .getJexlExpression()
            .getJexlExpression()
            .contains("simple_aggregation"));
  }

  @Test
  void buildSpanAttributes_detectionSignatureIncludesGroupByVariableName() {
    AbusePolicyData data = buildSimplePolicyData("entity_ip", "entity_ip");

    List<SpanAttributeDecoration> attrs = converter.buildSpanAttributes(data, Map.of());

    String detectionSigJexl =
        attrs.get(4).getSpanAttributeValue().getJexlExpression().getJexlExpression();
    // detection_signature references variable name, not inline JEXL
    assertTrue(detectionSigJexl.contains("entity_ip"));
    assertTrue(detectionSigJexl.contains("Group By Policy"));
  }

  // --- Helper ---

  private AbusePolicyData buildSimplePolicyData(
      String aggregationEntityId, String groupByEntityId) {
    return AbusePolicyData.newBuilder()
        .setName("Group By Policy")
        .setEnabled(true)
        .setSeverity(AbuseRiskSeverity.ABUSE_RISK_SEVERITY_MEDIUM)
        .setAction(
            AbuseActionConfig.newBuilder().setActionType(AbuseActionType.ABUSE_ACTION_TYPE_BLOCK))
        .setMessageFormat("alert")
        .setSimpleAggregationTemplate(
            AbuseSimpleAggregationTemplateConfig.newBuilder()
                .setAggregation(
                    AbuseAggregationConfig.newBuilder()
                        .setAggregationFunction(
                            AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT)
                        .setDerivedEntityId(aggregationEntityId))
                .setGroupBy(AbuseGroupByConfig.newBuilder().setDerivedEntityId(groupByEntityId))
                .setThreshold(
                    AbuseThresholdConfig.newBuilder()
                        .setOperator(AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_GREATER_THAN)
                        .setValue(10))
                .setTimeWindow(
                    AbuseTimeWindow.newBuilder()
                        .setLookbackDuration(Duration.newBuilder().setSeconds(300))))
        .build();
  }
}
