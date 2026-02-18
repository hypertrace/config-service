package ai.traceable.fraud.datamodel.event.kind.aggregationfunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.fraud.datamodel.event.kind.eventkind.DefaultEventKindProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunction;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionsByKind;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultAggregationFunctionProviderTest {

  private DefaultAggregationFunctionProvider provider;

  @BeforeEach
  void setUp() {
    EventKindHierarchyResolver resolver =
        new EventKindHierarchyResolver(new DefaultEventKindProvider());
    provider = new DefaultAggregationFunctionProvider(resolver);
  }

  @Test
  void getFunctionsByKinds_withValueKind_returnsUniversalFunctions() {
    ComplexDataModelEventKind valueKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_value").build();

    List<AggregationFunctionsByKind> result = provider.getFunctionsByKinds(List.of(valueKind));

    assertFalse(result.isEmpty(), "Should return functions for value kind");
    assertEquals(valueKind, result.get(0).getKind());

    // COUNT and DISTINCT_COUNT work on all types
    assertTrue(
        result.get(0).getFunctionsList().stream()
            .anyMatch(f -> f.getId().equals("system_aggregation_count")),
        "Should include COUNT for value kind");
  }

  @Test
  void getFunctionsByKinds_withNumericKind_returnsNumericAndInheritedFunctions() {
    ComplexDataModelEventKind numericKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_numeric").build();

    List<AggregationFunctionsByKind> result = provider.getFunctionsByKinds(List.of(numericKind));

    assertFalse(result.isEmpty(), "Should return functions for numeric kind");

    List<String> functionIds =
        result.get(0).getFunctionsList().stream()
            .map(AggregationFunction::getId)
            .collect(Collectors.toList());

    // Numeric-specific functions
    assertTrue(functionIds.contains("system_aggregation_sum"), "Should include SUM for numeric");
    assertTrue(functionIds.contains("system_aggregation_avg"), "Should include AVG for numeric");

    // Inherited from value kind (hierarchy resolution)
    assertTrue(
        functionIds.contains("system_aggregation_count"),
        "Should include COUNT inherited from value kind");
  }

  @Test
  void getFunctionsByKinds_withEmptyList_returnsEmpty() {
    List<AggregationFunctionsByKind> result = provider.getFunctionsByKinds(List.of());

    assertTrue(result.isEmpty(), "Should return empty for empty input");
  }

  @Test
  void getFunctionsByKinds_withMultipleKinds_returnsMultipleMappings() {
    ComplexDataModelEventKind numericKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_numeric").build();
    ComplexDataModelEventKind timestampKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_timestamp").build();

    List<AggregationFunctionsByKind> result =
        provider.getFunctionsByKinds(List.of(numericKind, timestampKind));

    assertEquals(2, result.size(), "Should return mappings for both kinds");
  }

  @Test
  void containsExpectedCoreFunctions() {
    ComplexDataModelEventKind numericKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_numeric").build();

    List<AggregationFunctionsByKind> numericResult =
        provider.getFunctionsByKinds(List.of(numericKind));

    List<String> numericFunctionIds =
        numericResult.get(0).getFunctionsList().stream()
            .map(AggregationFunction::getId)
            .collect(Collectors.toList());

    // Core functions that must exist for numeric (including inherited)
    assertTrue(numericFunctionIds.contains("system_aggregation_count"), "Must have COUNT function");
    assertTrue(numericFunctionIds.contains("system_aggregation_avg"), "Must have AVG function");
    assertTrue(numericFunctionIds.contains("system_aggregation_sum"), "Must have SUM function");
  }
}
