package ai.traceable.fraud.datamodel.event.kind.transformationfunction;

import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindProvider;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.EventKindFilter;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunction;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionsByKind;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.util.JsonFormat;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Default implementation that loads transformation functions from YAML resource file. */
@Slf4j
@Singleton
public class DefaultTransformationFunctionProvider implements TransformationFunctionProvider {

  private static final String RESOURCE_FILE = "transformation_functions.yaml";
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  private final List<TransformationFunction> transformationFunctions;
  private final EventKindHierarchyResolver hierarchyResolver;

  @Inject
  public DefaultTransformationFunctionProvider(
      EventKindHierarchyResolver hierarchyResolver, EventKindProvider eventKindProvider) {
    this.hierarchyResolver = hierarchyResolver;
    this.transformationFunctions = loadFunctions(eventKindProvider);
  }

  @Override
  public List<TransformationFunctionsByKind> getFunctionsByKinds(
      List<ComplexDataModelEventKind> requestedKinds) {
    List<TransformationFunctionsByKind> result = new ArrayList<>();

    for (ComplexDataModelEventKind kind : requestedKinds) {
      List<TransformationFunction> applicableFunctions =
          transformationFunctions.stream()
              .filter(func -> isCompatibleWithKind(func, kind))
              .collect(Collectors.toList());
      if (!applicableFunctions.isEmpty()) {
        result.add(
            TransformationFunctionsByKind.newBuilder()
                .setKind(kind)
                .addAllFunctions(applicableFunctions)
                .build());
      }
    }

    return result;
  }

  @Override
  public List<TransformationFunctionsByKind> getAllFunctions() {
    Map<ComplexDataModelEventKind, List<TransformationFunction>> byKind = new HashMap<>();
    for (TransformationFunction func : transformationFunctions) {
      for (ComplexDataModelEventKind inputKind : func.getInputKindsList()) {
        byKind.computeIfAbsent(inputKind, k -> new ArrayList<>()).add(func);
      }
    }
    return byKind.entrySet().stream()
        .map(
            e ->
                TransformationFunctionsByKind.newBuilder()
                    .setKind(e.getKey())
                    .addAllFunctions(e.getValue())
                    .build())
        .collect(Collectors.toList());
  }

  private boolean isCompatibleWithKind(
      TransformationFunction function, ComplexDataModelEventKind requestedKind) {
    for (ComplexDataModelEventKind inputKind : function.getInputKindsList()) {
      if (hierarchyResolver.isCompatible(inputKind, requestedKind)) {
        return true;
      }
    }
    return false;
  }

  private List<TransformationFunction> loadFunctions(EventKindProvider eventKindProvider) {
    try {
      JsonNode root =
          YAML_MAPPER.readValue(
              getClass().getClassLoader().getResourceAsStream(RESOURCE_FILE), JsonNode.class);
      JsonNode functionsNode = root.get("functions");

      if (functionsNode == null || !functionsNode.isArray()) {
        log.error("Invalid YAML structure in {}: missing 'functions' array", RESOURCE_FILE);
        return Collections.emptyList();
      }

      List<TransformationFunction> result = new ArrayList<>();
      for (JsonNode funcNode : functionsNode) {
        String json = JSON_MAPPER.writeValueAsString(funcNode);
        TransformationFunction.Builder builder = TransformationFunction.newBuilder();
        JSON_PARSER.merge(json, builder);
        result.add(builder.build());
      }
      appendTypeCastFunctions(result, eventKindProvider);
      return Collections.unmodifiableList(result);
    } catch (Exception e) {
      log.error("Failed to load transformation functions from {}", RESOURCE_FILE, e);
      return Collections.emptyList();
    }
  }

  /**
   * Appends {@code type_cast_to_<event_kind_id>} for every loaded kind. JEXL is pass-through only;
   * {@code validation_regex} on kinds is not evaluated here (can be enforced elsewhere later).
   */
  private static void appendTypeCastFunctions(
      List<TransformationFunction> to, EventKindProvider kinds) {
    ComplexDataModelEventKind stringIn =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();
    for (var k : kinds.getEventKinds(EventKindFilter.getDefaultInstance())) {
      String id = k.getId();
      to.add(
          TransformationFunction.newBuilder()
              .setId("type_cast_to_" + id)
              .setDisplayName("Cast to " + k.getDisplayName())
              .setDescription("Narrow to " + k.getDisplayName() + " for type safety.")
              .addInputKinds(stringIn)
              .setOutputKind(ComplexDataModelEventKind.newBuilder().setKindId(id).build())
              .setJexlTemplate("(${input})")
              .build());
    }
  }
}
