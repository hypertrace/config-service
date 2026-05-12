package ai.traceable.fraud.policy.config.service.converter;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.v1.SpanAttributeDecoration;
import ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionType;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyDetectionFilter;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdOperator;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Shared utility methods for template edge decision converters. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EdgeDecisionConverterUtils {

  public static AttributeDerivationMapping buildJexlAttribute(String name, String jexlExpression) {
    return AttributeDerivationMapping.newBuilder()
        .setName(name)
        .setType(FieldType.FIELD_TYPE_UNSPECIFIED)
        .addRules(
            DerivationRule.newBuilder()
                .setTransformationConfig(
                    DataTransformationConfig.newBuilder()
                        .setOutputType(FieldType.FIELD_TYPE_STR)
                        .setJexlExpression(
                            JexlExpressionConfig.newBuilder().setJexlExpression(jexlExpression))))
        .build();
  }

  /** Builds a VariableDerivationMapping from pre-built DerivationRules. */
  public static VariableDerivationMapping buildVariable(String name, List<DerivationRule> rules) {
    return VariableDerivationMapping.newBuilder().setName(name).addAllRules(rules).build();
  }

  /** Builds an AttributeDerivationMapping that references a variable by name. */
  public static AttributeDerivationMapping buildVariableRefAttribute(String variableName) {
    return AttributeDerivationMapping.newBuilder()
        .setName(variableName)
        .setType(FieldType.FIELD_TYPE_UNSPECIFIED)
        .addRules(
            DerivationRule.newBuilder()
                .setTransformationConfig(
                    DataTransformationConfig.newBuilder()
                        .setJexlExpression(
                            JexlExpressionConfig.newBuilder().setJexlExpression(variableName))))
        .build();
  }

  public static SpanAttributeDecoration buildJexlSpanAttribute(String key, String jexlExpression) {
    return SpanAttributeDecoration.newBuilder()
        .setSpanAttributeKey(
            DataTransformationConfig.newBuilder()
                .setStaticValue(Value.newBuilder().setStringValue(key)))
        .setSpanAttributeValue(
            DataTransformationConfig.newBuilder()
                .setJexlExpression(
                    JexlExpressionConfig.newBuilder().setJexlExpression(jexlExpression)))
        .build();
  }

  public static SpanAttributeDecoration buildStaticSpanAttribute(String key, String value) {
    return SpanAttributeDecoration.newBuilder()
        .setSpanAttributeKey(
            DataTransformationConfig.newBuilder()
                .setStaticValue(Value.newBuilder().setStringValue(key)))
        .setSpanAttributeValue(
            DataTransformationConfig.newBuilder()
                .setStaticValue(Value.newBuilder().setStringValue(value)))
        .build();
  }

  public static ValueAggregateThreshold.AggregationType convertAggregationFunctionType(
      AggregationFunctionType functionType) {
    switch (functionType) {
      case AGGREGATION_FUNCTION_TYPE_COUNT:
        return ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_COUNT;
      case AGGREGATION_FUNCTION_TYPE_DISTINCT_COUNT:
        return ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_DISTINCT_COUNT;
      case AGGREGATION_FUNCTION_TYPE_SUM:
        return ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_SUM;
      case AGGREGATION_FUNCTION_TYPE_AVG:
        return ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_AVG;
      case AGGREGATION_FUNCTION_TYPE_MAX:
        return ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_MAX;
      case AGGREGATION_FUNCTION_TYPE_MIN:
        return ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_MIN;
      default:
        return ValueAggregateThreshold.AggregationType.AGGREGATION_TYPE_COUNT;
    }
  }

  public static Set<String> getFilterDerivedEntityIds(AbusePolicyDetectionFilter filter) {
    if (filter.hasRelationalFilter()) {
      String entityId = filter.getRelationalFilter().getDerivedEntityId();
      return entityId.isEmpty() ? Set.of() : Set.of(entityId);
    } else if (filter.hasLogicalFilter()) {
      return filter.getLogicalFilter().getOperandsList().stream()
          .map(EdgeDecisionConverterUtils::getFilterDerivedEntityIds)
          .flatMap(Set::stream)
          .collect(Collectors.toSet());
    }
    return Set.of();
  }

  // EDS only supports ABOVE / BELOW threshold operators. GREATER_THAN and GREATER_THAN_OR_EQUAL
  // both map to ABOVE; LESS_THAN and LESS_THAN_OR_EQUAL both map to BELOW.
  // EQUAL, GREATER_THAN_OR_EQUAL, and LESS_THAN_OR_EQUAL are deprecated in the proto —
  // only GREATER_THAN and LESS_THAN should be used going forward.
  public static ValueAggregateThreshold.ThresholdOperator convertThresholdOperator(
      AbuseThresholdOperator operator) {
    switch (operator) {
      case ABUSE_THRESHOLD_OPERATOR_GREATER_THAN:
      case ABUSE_THRESHOLD_OPERATOR_GREATER_THAN_OR_EQUAL:
        return ValueAggregateThreshold.ThresholdOperator.THRESHOLD_OPERATOR_ABOVE;
      case ABUSE_THRESHOLD_OPERATOR_LESS_THAN:
      case ABUSE_THRESHOLD_OPERATOR_LESS_THAN_OR_EQUAL:
        return ValueAggregateThreshold.ThresholdOperator.THRESHOLD_OPERATOR_BELOW;
      default:
        return ValueAggregateThreshold.ThresholdOperator.THRESHOLD_OPERATOR_ABOVE;
    }
  }
}
