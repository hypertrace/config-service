package ai.traceable.fraud.datamodel.event.kind.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.fraud.datamodel.event.kind.eventkind.DefaultEventKindProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.Operator;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorType;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorsByKind;
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

class DefaultOperatorProviderTest {

  private static final ComplexDataModelEventKind STRING_KIND =
      ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();

  private DefaultOperatorProvider provider;

  @BeforeEach
  void setUp() {
    EventKindHierarchyResolver resolver =
        new EventKindHierarchyResolver(new DefaultEventKindProvider());
    provider = new DefaultOperatorProvider(resolver);
  }

  @Test
  void getOperatorsByKinds_withStringKind_returnsOperators() {
    List<OperatorsByKind> result = provider.getOperatorsByKinds(List.of(STRING_KIND));

    assertFalse(result.isEmpty(), "Should return operators for string kind");

    List<OperatorType> operatorTypes =
        result.get(0).getOperatorsList().stream()
            .map(Operator::getOperatorType)
            .collect(Collectors.toList());

    assertTrue(operatorTypes.contains(OperatorType.OPERATOR_TYPE_CONTAINS));
    assertTrue(operatorTypes.contains(OperatorType.OPERATOR_TYPE_STRING_EQUALS));
  }

  @Test
  void getOperatorsByKinds_allOperatorsHaveValidType() {
    List<OperatorsByKind> result = provider.getOperatorsByKinds(List.of(STRING_KIND));

    assertFalse(result.isEmpty());
    assertTrue(
        result.get(0).getOperatorsList().stream()
            .allMatch(op -> op.getOperatorType() != OperatorType.OPERATOR_TYPE_UNSPECIFIED),
        "Operators list should only contain valid operator types");
  }

  @Test
  void yamlOperatorsHaveExactOneToOneMappingWithEnum() throws Exception {
    ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    ObjectMapper jsonMapper = new ObjectMapper();
    JsonFormat.Parser jsonParser = JsonFormat.parser().ignoringUnknownFields();

    JsonNode root =
        yamlMapper.readValue(
            getClass().getClassLoader().getResourceAsStream("operators.yaml"), JsonNode.class);
    JsonNode functionsNode = root.get("functions");

    List<OperatorType> yamlOperatorTypes = new ArrayList<>();
    for (JsonNode funcNode : functionsNode) {
      String json = jsonMapper.writeValueAsString(funcNode);
      Operator.Builder builder = Operator.newBuilder();
      jsonParser.merge(json, builder);
      yamlOperatorTypes.add(builder.getOperatorType());
    }

    // All valid enum values (excluding UNSPECIFIED and UNRECOGNIZED)
    Set<OperatorType> allEnumValues =
        Arrays.stream(OperatorType.values())
            .filter(t -> t != OperatorType.OPERATOR_TYPE_UNSPECIFIED)
            .filter(t -> t != OperatorType.UNRECOGNIZED)
            .collect(Collectors.toSet());

    // Every enum value must have exactly one YAML entry
    Map<OperatorType, Long> yamlTypeCounts =
        yamlOperatorTypes.stream()
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    for (OperatorType enumValue : allEnumValues) {
      assertTrue(
          yamlTypeCounts.containsKey(enumValue),
          "Enum value " + enumValue + " is missing from operators.yaml");
      assertEquals(
          1L,
          yamlTypeCounts.get(enumValue),
          "Enum value " + enumValue + " has duplicate entries in operators.yaml");
    }
  }
}
