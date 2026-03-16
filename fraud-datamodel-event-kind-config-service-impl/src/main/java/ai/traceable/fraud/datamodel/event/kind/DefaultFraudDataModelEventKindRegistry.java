package ai.traceable.fraud.datamodel.event.kind;

import ai.traceable.fraud.datamodel.event.kind.aggregationfunction.AggregationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.operator.OperatorProvider;
import ai.traceable.fraud.datamodel.event.kind.transformationfunction.TransformationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunction;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionType;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.DataType;
import ai.traceable.fraud.datamodel.event.kind.v1.FraudDataModelEventKindRegistry;
import ai.traceable.fraud.datamodel.event.kind.v1.Operator;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorType;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunction;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionInvocation;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionsByKind;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationPipeline;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of FraudDataModelEventKindRegistry that wraps existing providers and
 * provides validation and compatibility checking functionality.
 */
@Singleton
public class DefaultFraudDataModelEventKindRegistry implements FraudDataModelEventKindRegistry {

  private final EventKindHierarchyResolver hierarchyResolver;
  private final Map<String, TransformationFunction> transformationFunctionById;
  private final Map<OperatorType, Operator> operatorByType;
  private final Map<AggregationFunctionType, AggregationFunction> aggregationFunctionByType;

  @Inject
  public DefaultFraudDataModelEventKindRegistry(
      EventKindHierarchyResolver hierarchyResolver,
      TransformationFunctionProvider transformationFunctionProvider,
      OperatorProvider operatorProvider,
      AggregationFunctionProvider aggregationFunctionProvider) {
    this.hierarchyResolver = hierarchyResolver;
    this.transformationFunctionById =
        buildTransformationFunctionMap(transformationFunctionProvider);
    this.operatorByType = buildOperatorMap(operatorProvider);
    this.aggregationFunctionByType = buildAggregationFunctionMap(aggregationFunctionProvider);
  }

  private Map<String, TransformationFunction> buildTransformationFunctionMap(
      TransformationFunctionProvider provider) {
    Map<String, TransformationFunction> result = new HashMap<>();
    for (TransformationFunctionsByKind byKind : provider.getAllFunctions()) {
      for (TransformationFunction func : byKind.getFunctionsList()) {
        result.put(func.getId(), func);
      }
    }
    return result;
  }

  private Map<OperatorType, Operator> buildOperatorMap(OperatorProvider provider) {
    Map<OperatorType, Operator> result = new EnumMap<>(OperatorType.class);
    for (Operator op : provider.getAllOperators()) {
      result.put(op.getOperatorType(), op);
    }
    return result;
  }

  private Map<AggregationFunctionType, AggregationFunction> buildAggregationFunctionMap(
      AggregationFunctionProvider provider) {
    Map<AggregationFunctionType, AggregationFunction> result =
        new EnumMap<>(AggregationFunctionType.class);
    for (AggregationFunction func : provider.getAllFunctions()) {
      result.put(func.getFunctionType(), func);
    }
    return result;
  }

  @Override
  public boolean isKindCompatible(
      ComplexDataModelEventKind functionInputKind, ComplexDataModelEventKind requestedKind) {
    return hierarchyResolver.isCompatible(functionInputKind, requestedKind);
  }

  @Override
  public boolean transformationFunctionExists(String functionId) {
    return transformationFunctionById.containsKey(functionId);
  }

  @Override
  public ComplexDataModelEventKind validateTransformationPipeline(
      ComplexDataModelEventKind inputKind, TransformationPipeline pipeline) {
    if (pipeline == null || pipeline.getTransformationPipelineCount() == 0) {
      return inputKind;
    }

    ComplexDataModelEventKind currentKind = inputKind;

    for (TransformationFunctionInvocation invocation : pipeline.getTransformationPipelineList()) {
      String functionId = invocation.getFunctionId();
      TransformationFunction function = transformationFunctionById.get(functionId);

      if (function == null) {
        throw new IllegalArgumentException("Transformation function not found: " + functionId);
      }

      if (!isCompatibleWithAnyInputKind(function, currentKind)) {
        throw new IllegalArgumentException(
            String.format(
                "Transformation function '%s' not compatible with input type '%s'",
                functionId, formatKind(currentKind)));
      }

      currentKind = function.getOutputKind();
    }

    return currentKind;
  }

  private boolean isCompatibleWithAnyInputKind(
      TransformationFunction function, ComplexDataModelEventKind kind) {
    for (ComplexDataModelEventKind inputKind : function.getInputKindsList()) {
      if (hierarchyResolver.isCompatible(inputKind, kind)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isOperatorCompatibleWithKind(
      OperatorType operatorType, ComplexDataModelEventKind kind) {
    if (operatorType == OperatorType.OPERATOR_TYPE_UNSPECIFIED) {
      return false;
    }

    Operator operator = operatorByType.get(operatorType);
    if (operator == null) {
      return false;
    }

    for (ComplexDataModelEventKind inputKind : operator.getInputKindsList()) {
      if (hierarchyResolver.isCompatible(inputKind, kind)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isAggregationFunctionCompatibleWithKind(
      AggregationFunctionType functionType, ComplexDataModelEventKind kind) {
    if (functionType == AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_UNSPECIFIED) {
      return false;
    }

    AggregationFunction function = aggregationFunctionByType.get(functionType);
    if (function == null) {
      return false;
    }

    for (ComplexDataModelEventKind inputKind : function.getInputKindsList()) {
      if (hierarchyResolver.isCompatible(inputKind, kind)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isLiteralValueCompatible(ComplexDataModelEventKind kind, Value literalValue) {
    if (kind.hasKindId()) {
      DataType dataType =
          hierarchyResolver.getDataType(kind.getKindId()).orElse(DataType.DATA_TYPE_STRING);

      switch (dataType) {
        case DATA_TYPE_STRING:
          return literalValue.hasStringValue();
        case DATA_TYPE_INT:
        case DATA_TYPE_LONG:
        case DATA_TYPE_DOUBLE:
        case DATA_TYPE_TIMESTAMP:
          return literalValue.hasNumberValue();
        case DATA_TYPE_BOOL:
          return literalValue.hasBoolValue();
        default:
          return true;
      }
    }

    if (kind.hasArrayOf()) {
      if (!literalValue.hasListValue()) {
        return false;
      }
      return literalValue.getListValue().getValuesList().stream()
          .allMatch(v -> isLiteralValueCompatible(kind.getArrayOf(), v));
    }

    return false;
  }

  private String formatKind(ComplexDataModelEventKind kind) {
    if (kind.hasKindId()) {
      return kind.getKindId();
    }
    if (kind.hasArrayOf()) {
      return "array<" + formatKind(kind.getArrayOf()) + ">";
    }
    return kind.toString();
  }
}
