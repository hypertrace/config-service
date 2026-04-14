package ai.traceable.fraud.datamodel.event.kind.transformationfunction;

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
    EventKindHierarchyResolver resolver =
        new EventKindHierarchyResolver(new DefaultEventKindProvider());
    provider = new DefaultTransformationFunctionProvider(resolver);
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
}
