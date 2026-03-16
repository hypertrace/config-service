package ai.traceable.fraud.datamodel.event.kind;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.fraud.datamodel.event.kind.aggregationfunction.AggregationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.operator.OperatorProvider;
import ai.traceable.fraud.datamodel.event.kind.transformationfunction.TransformationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.DataType;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
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
}
