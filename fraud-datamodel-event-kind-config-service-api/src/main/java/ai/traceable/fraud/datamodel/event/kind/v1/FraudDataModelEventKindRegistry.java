package ai.traceable.fraud.datamodel.event.kind.v1;

import com.google.protobuf.Value;

/**
 * Registry for validating fraud data model entities, transformation pipelines, operators, and
 * aggregation functions.
 *
 * <p>This interface provides validation and compatibility checks for event kinds, transformation
 * functions, operators, and aggregation functions. Implementations may load from static YAML files
 * or dynamic user-defined sources.
 */
public interface FraudDataModelEventKindRegistry {

  /**
   * Checks if a requested event kind is compatible with a function's expected input kind.
   *
   * @param functionInputKind the kind expected by a function/operator
   * @param requestedKind the kind being validated
   * @return true if requestedKind is compatible with functionInputKind
   */
  boolean isKindCompatible(
      ComplexDataModelEventKind functionInputKind, ComplexDataModelEventKind requestedKind);

  /**
   * Checks if a transformation function with the given ID exists.
   *
   * @param functionId the transformation function ID to check
   * @return true if the transformation function exists, false otherwise
   */
  boolean transformationFunctionExists(String functionId);

  /**
   * Validates a transformation pipeline and computes the output kind.
   *
   * <p>This method checks that all transformation functions in the pipeline exist and are
   * compatible with each other (output of one function matches input of the next). It returns the
   * final output kind if the pipeline is valid.
   *
   * @param inputKind the input kind to the pipeline
   * @param pipeline the transformation pipeline to validate
   * @return the output kind after applying all transformations
   * @throws IllegalArgumentException if the pipeline is invalid (function doesn't exist or types
   *     are incompatible)
   */
  ComplexDataModelEventKind validateTransformationPipeline(
      ComplexDataModelEventKind inputKind, TransformationPipeline pipeline);

  /**
   * Checks if an operator is compatible with the given event kind.
   *
   * @param operator the operator type to check
   * @param kind the event kind to validate against
   * @return true if the operator can be applied to values of the given kind
   */
  boolean isOperatorCompatibleWithKind(OperatorType operator, ComplexDataModelEventKind kind);

  /**
   * Checks if an aggregation function is compatible with the given event kind.
   *
   * @param functionType the aggregation function type to check
   * @param kind the event kind to validate against
   * @return true if the aggregation function can be applied to values of the given kind
   */
  boolean isAggregationFunctionCompatibleWithKind(
      AggregationFunctionType functionType, ComplexDataModelEventKind kind);

  /**
   * Checks if a literal value is compatible with the given event kind.
   *
   * @param kind the event kind to validate against
   * @param literalValue the literal value to check
   * @return true if the literal value type matches the expected type for the kind
   */
  boolean isLiteralValueCompatible(ComplexDataModelEventKind kind, Value literalValue);
}
