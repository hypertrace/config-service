package ai.traceable.fraud.datamodel.event.kind.aggregationfunction;

import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunction;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionsByKind;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.util.JsonFormat;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Default implementation that loads aggregation functions from YAML resource file. */
@Slf4j
@Singleton
public class DefaultAggregationFunctionProvider implements AggregationFunctionProvider {

  private static final String RESOURCE_FILE = "aggregation_functions.yaml";
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  private final List<AggregationFunction> functions;
  private final EventKindHierarchyResolver hierarchyResolver;

  @Inject
  public DefaultAggregationFunctionProvider(EventKindHierarchyResolver hierarchyResolver) {
    this.hierarchyResolver = hierarchyResolver;
    this.functions = loadFunctions();
  }

  @Override
  public List<AggregationFunctionsByKind> getFunctionsByKinds(
      List<ComplexDataModelEventKind> requestedKinds) {
    List<AggregationFunctionsByKind> result = new ArrayList<>();

    for (ComplexDataModelEventKind kind : requestedKinds) {
      List<AggregationFunction> applicableFunctions = findFunctionsForKind(kind);
      if (!applicableFunctions.isEmpty()) {
        result.add(
            AggregationFunctionsByKind.newBuilder()
                .setKind(kind)
                .addAllFunctions(applicableFunctions)
                .build());
      }
    }

    return result;
  }

  @Override
  public List<AggregationFunction> getAllFunctions() {
    return functions;
  }

  private List<AggregationFunction> findFunctionsForKind(ComplexDataModelEventKind kind) {
    return functions.stream()
        .filter(func -> isCompatibleWithKind(func, kind))
        .collect(Collectors.toList());
  }

  private boolean isCompatibleWithKind(
      AggregationFunction function, ComplexDataModelEventKind requestedKind) {
    for (ComplexDataModelEventKind inputKind : function.getInputKindsList()) {
      if (hierarchyResolver.isCompatible(inputKind, requestedKind)) {
        return true;
      }
    }
    return false;
  }

  private List<AggregationFunction> loadFunctions() {
    try {
      JsonNode root =
          YAML_MAPPER.readValue(
              getClass().getClassLoader().getResourceAsStream(RESOURCE_FILE), JsonNode.class);
      JsonNode functionsNode = root.get("functions");

      if (functionsNode == null || !functionsNode.isArray()) {
        log.error("Invalid YAML structure in {}: missing 'functions' array", RESOURCE_FILE);
        return Collections.emptyList();
      }

      List<AggregationFunction> result = new ArrayList<>();
      for (JsonNode funcNode : functionsNode) {
        String json = JSON_MAPPER.writeValueAsString(funcNode);
        AggregationFunction.Builder builder = AggregationFunction.newBuilder();
        JSON_PARSER.merge(json, builder);
        result.add(builder.build());
      }
      return Collections.unmodifiableList(result);
    } catch (Exception e) {
      log.error("Failed to load aggregation functions from {}", RESOURCE_FILE, e);
      return Collections.emptyList();
    }
  }
}
