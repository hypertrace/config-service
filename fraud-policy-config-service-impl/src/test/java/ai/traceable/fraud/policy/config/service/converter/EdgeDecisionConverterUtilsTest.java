package ai.traceable.fraud.policy.config.service.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.edge.decision.config.service.v1.SpanAttributeDecoration;
import ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionType;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyDetectionFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLogicalFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLogicalOperator;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyRelationalFilter;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdOperator;
import java.util.Set;
import org.junit.jupiter.api.Test;

class EdgeDecisionConverterUtilsTest {

  // --- buildJexlAttribute ---

  @Test
  void buildJexlAttribute_setsNameAndJexlExpression() {
    AttributeDerivationMapping attr =
        EdgeDecisionConverterUtils.buildJexlAttribute("entity_ip", "$s.getIpAddress()");

    assertEquals("entity_ip", attr.getName());
    assertEquals(FieldType.FIELD_TYPE_UNSPECIFIED, attr.getType());
    assertEquals(1, attr.getRulesCount());
    assertEquals(
        FieldType.FIELD_TYPE_STR, attr.getRules(0).getTransformationConfig().getOutputType());
    assertEquals(
        "$s.getIpAddress()",
        attr.getRules(0).getTransformationConfig().getJexlExpression().getJexlExpression());
  }

  // --- buildJexlSpanAttribute ---

  @Test
  void buildJexlSpanAttribute_setsStaticKeyAndJexlValue() {
    SpanAttributeDecoration attr =
        EdgeDecisionConverterUtils.buildJexlSpanAttribute("actor", "$s.getIpAddress()");

    assertEquals("actor", attr.getSpanAttributeKey().getStaticValue().getStringValue());
    assertEquals(
        "$s.getIpAddress()", attr.getSpanAttributeValue().getJexlExpression().getJexlExpression());
  }

  // --- buildStaticSpanAttribute ---

  @Test
  void buildStaticSpanAttribute_setsStaticKeyAndStaticValue() {
    SpanAttributeDecoration attr =
        EdgeDecisionConverterUtils.buildStaticSpanAttribute("actor_type", "IP ADDRESS");

    assertEquals("actor_type", attr.getSpanAttributeKey().getStaticValue().getStringValue());
    assertEquals("IP ADDRESS", attr.getSpanAttributeValue().getStaticValue().getStringValue());
  }

  // --- convertAggregationFunctionType ---

  @Test
  void convertAggregationFunctionType_mapsAllKnownTypes() {
    assertEquals(
        ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_COUNT,
        EdgeDecisionConverterUtils.convertAggregationFunctionType(
            AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT));
    assertEquals(
        ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_DISTINCT_COUNT,
        EdgeDecisionConverterUtils.convertAggregationFunctionType(
            AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_DISTINCT_COUNT));
    assertEquals(
        ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_SUM,
        EdgeDecisionConverterUtils.convertAggregationFunctionType(
            AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_SUM));
    assertEquals(
        ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_AVG,
        EdgeDecisionConverterUtils.convertAggregationFunctionType(
            AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_AVG));
    assertEquals(
        ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_MAX,
        EdgeDecisionConverterUtils.convertAggregationFunctionType(
            AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_MAX));
    assertEquals(
        ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_MIN,
        EdgeDecisionConverterUtils.convertAggregationFunctionType(
            AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_MIN));
  }

  @Test
  void convertAggregationFunctionType_defaultsToCount() {
    assertEquals(
        ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_COUNT,
        EdgeDecisionConverterUtils.convertAggregationFunctionType(
            AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_UNSPECIFIED));
  }

  // --- convertThresholdOperator ---

  @Test
  void convertThresholdOperator_greaterThanMapsToAbove() {
    assertEquals(
        ValueAggregateThreshold.ThresholdOperator.THRESHOLD_OPERATOR_ABOVE,
        EdgeDecisionConverterUtils.convertThresholdOperator(
            AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_GREATER_THAN));
    assertEquals(
        ValueAggregateThreshold.ThresholdOperator.THRESHOLD_OPERATOR_ABOVE,
        EdgeDecisionConverterUtils.convertThresholdOperator(
            AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_GREATER_THAN_OR_EQUAL));
  }

  @Test
  void convertThresholdOperator_lessThanMapsToBelow() {
    assertEquals(
        ValueAggregateThreshold.ThresholdOperator.THRESHOLD_OPERATOR_BELOW,
        EdgeDecisionConverterUtils.convertThresholdOperator(
            AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_LESS_THAN));
    assertEquals(
        ValueAggregateThreshold.ThresholdOperator.THRESHOLD_OPERATOR_BELOW,
        EdgeDecisionConverterUtils.convertThresholdOperator(
            AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_LESS_THAN_OR_EQUAL));
  }

  @Test
  void convertThresholdOperator_equalDeprecated_mapsToAbove() {
    assertEquals(
        ValueAggregateThreshold.ThresholdOperator.THRESHOLD_OPERATOR_ABOVE,
        EdgeDecisionConverterUtils.convertThresholdOperator(
            AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_EQUAL));
  }

  @Test
  void convertThresholdOperator_defaultsToAbove() {
    assertEquals(
        ValueAggregateThreshold.ThresholdOperator.THRESHOLD_OPERATOR_ABOVE,
        EdgeDecisionConverterUtils.convertThresholdOperator(
            AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_UNSPECIFIED));
  }

  // --- getFilterDerivedEntityIds ---

  @Test
  void getFilterDerivedEntityIds_relationalFilter_returnsEntityId() {
    AbusePolicyDetectionFilter filter =
        AbusePolicyDetectionFilter.newBuilder()
            .setRelationalFilter(
                AbusePolicyRelationalFilter.newBuilder().setDerivedEntityId("entity_1"))
            .build();

    assertEquals(Set.of("entity_1"), EdgeDecisionConverterUtils.getFilterDerivedEntityIds(filter));
  }

  @Test
  void getFilterDerivedEntityIds_relationalFilter_emptyId_returnsEmptySet() {
    AbusePolicyDetectionFilter filter =
        AbusePolicyDetectionFilter.newBuilder()
            .setRelationalFilter(AbusePolicyRelationalFilter.newBuilder().setDerivedEntityId(""))
            .build();

    assertTrue(EdgeDecisionConverterUtils.getFilterDerivedEntityIds(filter).isEmpty());
  }

  @Test
  void getFilterDerivedEntityIds_logicalFilter_collectsFromOperands() {
    AbusePolicyDetectionFilter filter =
        AbusePolicyDetectionFilter.newBuilder()
            .setLogicalFilter(
                AbusePolicyLogicalFilter.newBuilder()
                    .setOperator(AbusePolicyLogicalOperator.ABUSE_POLICY_LOGICAL_OPERATOR_AND)
                    .addOperands(
                        AbusePolicyDetectionFilter.newBuilder()
                            .setRelationalFilter(
                                AbusePolicyRelationalFilter.newBuilder()
                                    .setDerivedEntityId("entity_a")))
                    .addOperands(
                        AbusePolicyDetectionFilter.newBuilder()
                            .setRelationalFilter(
                                AbusePolicyRelationalFilter.newBuilder()
                                    .setDerivedEntityId("entity_b"))))
            .build();

    assertEquals(
        Set.of("entity_a", "entity_b"),
        EdgeDecisionConverterUtils.getFilterDerivedEntityIds(filter));
  }

  @Test
  void getFilterDerivedEntityIds_nestedLogicalFilter_collectsRecursively() {
    AbusePolicyDetectionFilter innerFilter =
        AbusePolicyDetectionFilter.newBuilder()
            .setLogicalFilter(
                AbusePolicyLogicalFilter.newBuilder()
                    .setOperator(AbusePolicyLogicalOperator.ABUSE_POLICY_LOGICAL_OPERATOR_OR)
                    .addOperands(
                        AbusePolicyDetectionFilter.newBuilder()
                            .setRelationalFilter(
                                AbusePolicyRelationalFilter.newBuilder()
                                    .setDerivedEntityId("deep_entity"))))
            .build();

    AbusePolicyDetectionFilter outerFilter =
        AbusePolicyDetectionFilter.newBuilder()
            .setLogicalFilter(
                AbusePolicyLogicalFilter.newBuilder()
                    .setOperator(AbusePolicyLogicalOperator.ABUSE_POLICY_LOGICAL_OPERATOR_AND)
                    .addOperands(
                        AbusePolicyDetectionFilter.newBuilder()
                            .setRelationalFilter(
                                AbusePolicyRelationalFilter.newBuilder()
                                    .setDerivedEntityId("top_entity")))
                    .addOperands(innerFilter))
            .build();

    assertEquals(
        Set.of("top_entity", "deep_entity"),
        EdgeDecisionConverterUtils.getFilterDerivedEntityIds(outerFilter));
  }

  @Test
  void getFilterDerivedEntityIds_emptyFilter_returnsEmptySet() {
    AbusePolicyDetectionFilter filter = AbusePolicyDetectionFilter.getDefaultInstance();

    assertTrue(EdgeDecisionConverterUtils.getFilterDerivedEntityIds(filter).isEmpty());
  }
}
