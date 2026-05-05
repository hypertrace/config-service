package ai.traceable.fraud.datamodel.event.kind.transformationfunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.fraud.datamodel.event.kind.eventkind.DefaultEventKindProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunction;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionsByKind;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultTransformationFunctionProviderTest {

  private static final ComplexDataModelEventKind STRING_KIND =
      ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();

  private DefaultTransformationFunctionProvider provider;

  @BeforeEach
  void setUp() {
    DefaultEventKindProvider eventKinds = new DefaultEventKindProvider();
    EventKindHierarchyResolver resolver = new EventKindHierarchyResolver(eventKinds);
    provider = new DefaultTransformationFunctionProvider(resolver, eventKinds);
  }

  @Test
  void getFunctionsByKinds_withStringKind_returnsExpectedFunctions() {
    List<TransformationFunctionsByKind> result = provider.getFunctionsByKinds(List.of(STRING_KIND));

    assertFalse(result.isEmpty(), "Should return functions for string kind");

    List<String> functionIds =
        result.get(0).getFunctionsList().stream()
            .map(TransformationFunction::getId)
            .collect(Collectors.toList());

    // String-specific function
    assertTrue(functionIds.contains("system_defined_function_base64_decode"));
    // Inherited from value kind
    assertTrue(functionIds.contains("system_defined_function_to_string"));
    assertTrue(
        functionIds.contains("type_cast_to_system_event_kind_email"),
        "Generated type cast uses id type_cast_to_<event_kind_id>");
  }

  @Test
  void getFunctionsByKinds_withArrayKind_returnsGetFunction() {
    ComplexDataModelEventKind arrayKind =
        ComplexDataModelEventKind.newBuilder()
            .setArrayOf(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
            .build();

    List<TransformationFunctionsByKind> result = provider.getFunctionsByKinds(List.of(arrayKind));

    assertFalse(result.isEmpty());
    assertTrue(
        result.get(0).getFunctionsList().stream()
            .anyMatch(f -> f.getId().equals("system_defined_function_get")));
  }

  @Test
  void getAllFunctions_returnsNonEmptyResult() {
    List<TransformationFunctionsByKind> result = provider.getAllFunctions();

    assertFalse(result.isEmpty(), "Should return all function mappings");
    assertTrue(
        result.stream().allMatch(m -> !m.getFunctionsList().isEmpty()),
        "Each mapping should have at least one function");
  }

  @Test
  void getFunctionsByKinds_functionsDoNotContainOperators() {
    List<TransformationFunctionsByKind> functions =
        provider.getFunctionsByKinds(List.of(STRING_KIND));

    assertFalse(
        functions.get(0).getFunctionsList().stream().anyMatch(f -> f.getId().contains("operator")),
        "Functions list should not contain operators");
  }

  @Test
  void typeCastFunction_numericKind_usesToNum() {
    TransformationFunction integerCast = findFunction("type_cast_to_system_event_kind_integer");
    assertEquals("traceable:toNum(${input})", integerCast.getJexlTemplate());

    TransformationFunction floatCast = findFunction("type_cast_to_system_event_kind_float");
    assertEquals("traceable:toNum(${input})", floatCast.getJexlTemplate());

    TransformationFunction numericCast = findFunction("type_cast_to_system_event_kind_numeric");
    assertEquals("traceable:toNum(${input})", numericCast.getJexlTemplate());
  }

  @Test
  void typeCastFunction_stringKind_usesToStr() {
    TransformationFunction emailCast = findFunction("type_cast_to_system_event_kind_email");
    assertEquals("traceable:toStr(${input})", emailCast.getJexlTemplate());
  }

  @Test
  void typeCastFunction_boolAndTimestamp_usesToStr() {
    TransformationFunction boolCast = findFunction("type_cast_to_system_event_kind_boolean");
    assertEquals("traceable:toStr(${input})", boolCast.getJexlTemplate());

    TransformationFunction timestampCast = findFunction("type_cast_to_system_event_kind_timestamp");
    assertEquals("traceable:toStr(${input})", timestampCast.getJexlTemplate());
  }

  private TransformationFunction findFunction(String id) {
    return provider.getFunctionsByKinds(List.of(STRING_KIND)).get(0).getFunctionsList().stream()
        .filter(f -> f.getId().equals(id))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Function " + id + " not found"));
  }
}
