package ai.traceable.fraud.datamodel.event.kind;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.fraud.datamodel.event.kind.aggregationfunction.AggregationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindProvider;
import ai.traceable.fraud.datamodel.event.kind.operator.OperatorProvider;
import ai.traceable.fraud.datamodel.event.kind.transformationfunction.TransformationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.DataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.DataType;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunction;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionInvocation;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionsByKind;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationPipeline;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultFraudDataModelEventKindRegistryTest {

  @Mock private EventKindHierarchyResolver hierarchyResolver;
  @Mock private TransformationFunctionProvider transformationFunctionProvider;
  @Mock private OperatorProvider operatorProvider;
  @Mock private AggregationFunctionProvider aggregationFunctionProvider;

  private DefaultFraudDataModelEventKindRegistry registry;

  @BeforeEach
  void setUp() {
    when(transformationFunctionProvider.getAllFunctions()).thenReturn(List.of());
    when(operatorProvider.getAllOperators()).thenReturn(List.of());
    when(aggregationFunctionProvider.getAllFunctions()).thenReturn(List.of());

    registry =
        new DefaultFraudDataModelEventKindRegistry(
            hierarchyResolver,
            transformationFunctionProvider,
            operatorProvider,
            aggregationFunctionProvider);
  }

  @Test
  void testLiteralValueCompatible_StringKind_AcceptsStringLiteral() {
    ComplexDataModelEventKind stringKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();
    when(hierarchyResolver.getDataType("system_event_kind_string"))
        .thenReturn(Optional.of(DataType.DATA_TYPE_STRING));

    Value stringValue = Value.newBuilder().setStringValue("test").build();
    assertTrue(registry.isLiteralValueCompatible(stringKind, stringValue));
  }

  @Test
  void testLiteralValueCompatible_StringKind_RejectsNumberLiteral() {
    ComplexDataModelEventKind stringKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();
    when(hierarchyResolver.getDataType("system_event_kind_string"))
        .thenReturn(Optional.of(DataType.DATA_TYPE_STRING));

    Value numberValue = Value.newBuilder().setNumberValue(123).build();
    assertFalse(registry.isLiteralValueCompatible(stringKind, numberValue));
  }

  @Test
  void testLiteralValueCompatible_NumericKind_AcceptsNumberLiteral() {
    ComplexDataModelEventKind numericKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_numeric").build();
    when(hierarchyResolver.getDataType("system_event_kind_numeric"))
        .thenReturn(Optional.of(DataType.DATA_TYPE_DOUBLE));

    Value numberValue = Value.newBuilder().setNumberValue(123.45).build();
    assertTrue(registry.isLiteralValueCompatible(numericKind, numberValue));
  }

  @Test
  void testLiteralValueCompatible_NumericKind_RejectsStringLiteral() {
    ComplexDataModelEventKind numericKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_numeric").build();
    when(hierarchyResolver.getDataType("system_event_kind_numeric"))
        .thenReturn(Optional.of(DataType.DATA_TYPE_DOUBLE));

    Value stringValue = Value.newBuilder().setStringValue("not a number").build();
    assertFalse(registry.isLiteralValueCompatible(numericKind, stringValue));
  }

  @Test
  void testLiteralValueCompatible_TimestampKind_AcceptsNumberLiteral() {
    ComplexDataModelEventKind timestampKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_timestamp").build();
    when(hierarchyResolver.getDataType("system_event_kind_timestamp"))
        .thenReturn(Optional.of(DataType.DATA_TYPE_TIMESTAMP));

    Value numberValue = Value.newBuilder().setNumberValue(1709999999000L).build();
    assertTrue(registry.isLiteralValueCompatible(timestampKind, numberValue));
  }

  @Test
  void testLiteralValueCompatible_TimestampKind_RejectsStringLiteral() {
    ComplexDataModelEventKind timestampKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_timestamp").build();
    when(hierarchyResolver.getDataType("system_event_kind_timestamp"))
        .thenReturn(Optional.of(DataType.DATA_TYPE_TIMESTAMP));

    Value stringValue = Value.newBuilder().setStringValue("sdf").build();
    assertFalse(registry.isLiteralValueCompatible(timestampKind, stringValue));
  }

  @Test
  void testLiteralValueCompatible_BooleanKind_AcceptsBoolLiteral() {
    ComplexDataModelEventKind boolKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_boolean").build();
    when(hierarchyResolver.getDataType("system_event_kind_boolean"))
        .thenReturn(Optional.of(DataType.DATA_TYPE_BOOL));

    Value boolValue = Value.newBuilder().setBoolValue(true).build();
    assertTrue(registry.isLiteralValueCompatible(boolKind, boolValue));
  }

  @Test
  void testLiteralValueCompatible_BooleanKind_RejectsStringLiteral() {
    ComplexDataModelEventKind boolKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_boolean").build();
    when(hierarchyResolver.getDataType("system_event_kind_boolean"))
        .thenReturn(Optional.of(DataType.DATA_TYPE_BOOL));

    Value stringValue = Value.newBuilder().setStringValue("true").build();
    assertFalse(registry.isLiteralValueCompatible(boolKind, stringValue));
  }

  @Test
  void testLiteralValueCompatible_ArrayOfStrings_AcceptsStringList() {
    ComplexDataModelEventKind arrayKind =
        ComplexDataModelEventKind.newBuilder()
            .setArrayOf(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
            .build();
    when(hierarchyResolver.getDataType("system_event_kind_string"))
        .thenReturn(Optional.of(DataType.DATA_TYPE_STRING));

    Value listValue =
        Value.newBuilder()
            .setListValue(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("a"))
                    .addValues(Value.newBuilder().setStringValue("b")))
            .build();
    assertTrue(registry.isLiteralValueCompatible(arrayKind, listValue));
  }

  @Test
  void testLiteralValueCompatible_ArrayOfStrings_RejectsMixedList() {
    ComplexDataModelEventKind arrayKind =
        ComplexDataModelEventKind.newBuilder()
            .setArrayOf(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
            .build();
    when(hierarchyResolver.getDataType("system_event_kind_string"))
        .thenReturn(Optional.of(DataType.DATA_TYPE_STRING));

    Value listValue =
        Value.newBuilder()
            .setListValue(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("a"))
                    .addValues(Value.newBuilder().setNumberValue(123)))
            .build();
    assertFalse(registry.isLiteralValueCompatible(arrayKind, listValue));
  }

  /**
   * Tests for validateTransformationPipeline with type_parameter_ref resolution. Uses real
   * EventKindHierarchyResolver and real TransformationFunction objects to validate that generic
   * type parameters (e.g., T in array<T> → T) are resolved correctly during pipeline validation.
   */
  @Nested
  class ValidateTransformationPipelineTypeParameterTest {

    private DefaultFraudDataModelEventKindRegistry realRegistry;

    @BeforeEach
    void setUp() {
      // Real event kinds: value → string → email
      List<DataModelEventKind> kinds =
          List.of(
              DataModelEventKind.newBuilder()
                  .setId("system_event_kind_value")
                  .setDataType(DataType.DATA_TYPE_STRING)
                  .build(),
              DataModelEventKind.newBuilder()
                  .setId("system_event_kind_string")
                  .setParentKindId("system_event_kind_value")
                  .setDataType(DataType.DATA_TYPE_STRING)
                  .build(),
              DataModelEventKind.newBuilder()
                  .setId("system_event_kind_email")
                  .setParentKindId("system_event_kind_string")
                  .setDataType(DataType.DATA_TYPE_STRING)
                  .build());

      EventKindProvider kindProvider = filter -> kinds;
      EventKindHierarchyResolver realHierarchy = new EventKindHierarchyResolver(kindProvider);

      ComplexDataModelEventKind valueKind =
          ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_value").build();
      ComplexDataModelEventKind stringKind =
          ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();
      ComplexDataModelEventKind emailKind =
          ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_email").build();

      // split: string → array<string>
      TransformationFunction splitFn =
          TransformationFunction.newBuilder()
              .setId("system_defined_function_split")
              .addInputKinds(stringKind)
              .setOutputKind(ComplexDataModelEventKind.newBuilder().setArrayOf(stringKind))
              .build();

      // last: array<T> → T (generic)
      TransformationFunction lastFn =
          TransformationFunction.newBuilder()
              .setId("system_defined_function_last")
              .addInputKinds(
                  ComplexDataModelEventKind.newBuilder()
                      .setArrayOf(ComplexDataModelEventKind.newBuilder().setTypeParameterRef("T")))
              .setOutputKind(ComplexDataModelEventKind.newBuilder().setTypeParameterRef("T"))
              .build();

      // trim: string → string
      TransformationFunction trimFn =
          TransformationFunction.newBuilder()
              .setId("system_defined_function_trim")
              .addInputKinds(stringKind)
              .setOutputKind(stringKind)
              .build();

      // type_cast_to_email: string → email
      TransformationFunction castToEmailFn =
          TransformationFunction.newBuilder()
              .setId("type_cast_to_system_event_kind_email")
              .addInputKinds(stringKind)
              .setOutputKind(emailKind)
              .build();

      // parseJson: string → map<string, value>
      TransformationFunction parseJsonFn =
          TransformationFunction.newBuilder()
              .setId("system_defined_function_parse_json")
              .addInputKinds(stringKind)
              .setOutputKind(ComplexDataModelEventKind.newBuilder().setStringMapOf(valueKind))
              .build();

      // getEntry: map<string, T> → T (generic)
      TransformationFunction getEntryFn =
          TransformationFunction.newBuilder()
              .setId("system_defined_function_get_entry")
              .addInputKinds(
                  ComplexDataModelEventKind.newBuilder()
                      .setStringMapOf(
                          ComplexDataModelEventKind.newBuilder().setTypeParameterRef("T")))
              .setOutputKind(ComplexDataModelEventKind.newBuilder().setTypeParameterRef("T"))
              .build();

      TransformationFunctionsByKind byKind =
          TransformationFunctionsByKind.newBuilder()
              .addFunctions(splitFn)
              .addFunctions(lastFn)
              .addFunctions(trimFn)
              .addFunctions(castToEmailFn)
              .addFunctions(parseJsonFn)
              .addFunctions(getEntryFn)
              .build();

      when(transformationFunctionProvider.getAllFunctions()).thenReturn(List.of(byKind));
      when(operatorProvider.getAllOperators()).thenReturn(List.of());
      when(aggregationFunctionProvider.getAllFunctions()).thenReturn(List.of());

      realRegistry =
          new DefaultFraudDataModelEventKindRegistry(
              realHierarchy,
              transformationFunctionProvider,
              operatorProvider,
              aggregationFunctionProvider);
    }

    @Test
    void testPipeline_SplitLastTrimCast_ResolvesTypeParameterCorrectly() {
      // Pipeline: split → last → trim → type_cast_to_email
      // This is the exact pipeline from the failing mutation.
      // Without type_parameter_ref resolution, 'last' outputs raw T instead of string,
      // causing 'trim' to reject the input.
      TransformationPipeline pipeline =
          TransformationPipeline.newBuilder()
              .addTransformationPipeline(
                  TransformationFunctionInvocation.newBuilder()
                      .setFunctionId("system_defined_function_split"))
              .addTransformationPipeline(
                  TransformationFunctionInvocation.newBuilder()
                      .setFunctionId("system_defined_function_last"))
              .addTransformationPipeline(
                  TransformationFunctionInvocation.newBuilder()
                      .setFunctionId("system_defined_function_trim"))
              .addTransformationPipeline(
                  TransformationFunctionInvocation.newBuilder()
                      .setFunctionId("type_cast_to_system_event_kind_email"))
              .build();

      ComplexDataModelEventKind inputKind =
          ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();

      ComplexDataModelEventKind outputKind =
          realRegistry.validateTransformationPipeline(inputKind, pipeline);

      assertEquals("system_event_kind_email", outputKind.getKindId());
    }

    @Test
    void testPipeline_SplitLast_ResolvesTypeParameterToString() {
      // Pipeline: split → last
      // Should resolve T to string (from array<string>)
      TransformationPipeline pipeline =
          TransformationPipeline.newBuilder()
              .addTransformationPipeline(
                  TransformationFunctionInvocation.newBuilder()
                      .setFunctionId("system_defined_function_split"))
              .addTransformationPipeline(
                  TransformationFunctionInvocation.newBuilder()
                      .setFunctionId("system_defined_function_last"))
              .build();

      ComplexDataModelEventKind inputKind =
          ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();

      ComplexDataModelEventKind outputKind =
          realRegistry.validateTransformationPipeline(inputKind, pipeline);

      assertEquals("system_event_kind_string", outputKind.getKindId());
    }

    @Test
    void testPipeline_ParseJsonGetEntry_ResolvesMapTypeParameterCorrectly() {
      // Pipeline: parseJson → getEntry
      // parseJson outputs map<string, value>, getEntry (map<string, T> → T) should resolve
      // T to value. Without stringMapOf support, getEntry would output raw T.
      TransformationPipeline pipeline =
          TransformationPipeline.newBuilder()
              .addTransformationPipeline(
                  TransformationFunctionInvocation.newBuilder()
                      .setFunctionId("system_defined_function_parse_json"))
              .addTransformationPipeline(
                  TransformationFunctionInvocation.newBuilder()
                      .setFunctionId("system_defined_function_get_entry"))
              .build();

      ComplexDataModelEventKind inputKind =
          ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();

      ComplexDataModelEventKind outputKind =
          realRegistry.validateTransformationPipeline(inputKind, pipeline);

      assertEquals("system_event_kind_value", outputKind.getKindId());
    }
  }
}
