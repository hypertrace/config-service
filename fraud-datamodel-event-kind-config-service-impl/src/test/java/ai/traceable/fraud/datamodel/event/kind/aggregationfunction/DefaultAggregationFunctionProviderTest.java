package ai.traceable.fraud.datamodel.event.kind.aggregationfunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.fraud.datamodel.event.kind.eventkind.DefaultEventKindProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunction;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionType;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionsByKind;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.util.JsonFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
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
            .anyMatch(
                f ->
                    f.getFunctionType() == AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT),
        "Should include COUNT for value kind");
  }

  @Test
  void getFunctionsByKinds_withNumericKind_returnsNumericAndInheritedFunctions() {
    ComplexDataModelEventKind numericKind =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_numeric").build();

    List<AggregationFunctionsByKind> result = provider.getFunctionsByKinds(List.of(numericKind));

    assertFalse(result.isEmpty(), "Should return functions for numeric kind");

    List<AggregationFunctionType> functionTypes =
        result.get(0).getFunctionsList().stream()
            .map(AggregationFunction::getFunctionType)
            .collect(Collectors.toList());

    // Numeric-specific functions
    assertTrue(
        functionTypes.contains(AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_SUM),
        "Should include SUM for numeric");
    assertTrue(
        functionTypes.contains(AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_AVG),
        "Should include AVG for numeric");

    // Inherited from value kind (hierarchy resolution)
    assertTrue(
        functionTypes.contains(AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT),
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

    List<AggregationFunctionType> numericFunctionTypes =
        numericResult.get(0).getFunctionsList().stream()
            .map(AggregationFunction::getFunctionType)
            .collect(Collectors.toList());

    // Core functions that must exist for numeric (including inherited)
    assertTrue(
        numericFunctionTypes.contains(AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT),
        "Must have COUNT function");
    assertTrue(
        numericFunctionTypes.contains(AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_AVG),
        "Must have AVG function");
    assertTrue(
        numericFunctionTypes.contains(AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_SUM),
        "Must have SUM function");
  }

  @Test
  void yamlFunctionsHaveExactOneToOneMappingWithEnum() throws Exception {
    // Load functions from YAML (same way the provider does)
    ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    ObjectMapper jsonMapper = new ObjectMapper();
    JsonFormat.Parser jsonParser = JsonFormat.parser().ignoringUnknownFields();

    JsonNode root =
        yamlMapper.readValue(
            getClass().getClassLoader().getResourceAsStream("aggregation_functions.yaml"),
            JsonNode.class);
    JsonNode functionsNode = root.get("functions");

    List<AggregationFunctionType> yamlFunctionTypes = new ArrayList<>();
    for (JsonNode funcNode : functionsNode) {
      String json = jsonMapper.writeValueAsString(funcNode);
      AggregationFunction.Builder builder = AggregationFunction.newBuilder();
      jsonParser.merge(json, builder);
      yamlFunctionTypes.add(builder.getFunctionType());
    }

    // All valid enum values (excluding UNSPECIFIED and UNRECOGNIZED)
    Set<AggregationFunctionType> allEnumValues =
        Arrays.stream(AggregationFunctionType.values())
            .filter(t -> t != AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_UNSPECIFIED)
            .filter(t -> t != AggregationFunctionType.UNRECOGNIZED)
            .collect(Collectors.toSet());

    // Every enum value must have exactly one YAML entry
    Map<AggregationFunctionType, Long> yamlTypeCounts =
        yamlFunctionTypes.stream()
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    for (AggregationFunctionType enumValue : allEnumValues) {
      assertTrue(
          yamlTypeCounts.containsKey(enumValue),
          "Enum value " + enumValue + " is missing from aggregation_functions.yaml");
      assertEquals(
          1L,
          yamlTypeCounts.get(enumValue),
          "Enum value " + enumValue + " has duplicate entries in aggregation_functions.yaml");
    }
  }
}
