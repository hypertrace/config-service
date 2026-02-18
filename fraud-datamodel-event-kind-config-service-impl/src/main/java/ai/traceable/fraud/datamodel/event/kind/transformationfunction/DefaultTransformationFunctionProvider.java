package ai.traceable.fraud.datamodel.event.kind.transformationfunction;

import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorsByKind;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunction;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionsByKind;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.util.JsonFormat;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Default implementation that loads transformation functions from YAML resource files. */
@Slf4j
@Singleton
public class DefaultTransformationFunctionProvider implements TransformationFunctionProvider {

  private static final String FUNCTIONS_FILE = "transformation_functions.yaml";
  private static final String OPERATORS_FILE = "operators.yaml";
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  private final List<TransformationFunction> transformationFunctions;
  private final List<TransformationFunction> operators;
  private final EventKindHierarchyResolver hierarchyResolver;

  @Inject
  public DefaultTransformationFunctionProvider(EventKindHierarchyResolver hierarchyResolver) {
    this.hierarchyResolver = hierarchyResolver;
    this.transformationFunctions =
        Collections.unmodifiableList(loadFunctionsFromFile(FUNCTIONS_FILE));
    this.operators = Collections.unmodifiableList(loadFunctionsFromFile(OPERATORS_FILE));
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
  public List<OperatorsByKind> getOperatorsByKinds(List<ComplexDataModelEventKind> requestedKinds) {
    List<OperatorsByKind> result = new ArrayList<>();

    for (ComplexDataModelEventKind kind : requestedKinds) {
      List<TransformationFunction> applicableOperators =
          operators.stream()
              .filter(op -> isCompatibleWithKind(op, kind))
              .collect(Collectors.toList());
      if (!applicableOperators.isEmpty()) {
        result.add(
            OperatorsByKind.newBuilder()
                .setKind(kind)
                .addAllOperators(applicableOperators)
                .build());
      }
    }

    return result;
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

  private List<TransformationFunction> loadFunctionsFromFile(String filename) {
    try {
      InputStream stream = getClass().getClassLoader().getResourceAsStream(filename);
      if (stream == null) {
        log.warn("Resource file not found: {}", filename);
        return Collections.emptyList();
      }

      JsonNode root = YAML_MAPPER.readValue(stream, JsonNode.class);
      JsonNode functionsNode = root.get("functions");

      if (functionsNode == null || !functionsNode.isArray()) {
        log.error("Invalid YAML structure in {}: missing 'functions' array", filename);
        return Collections.emptyList();
      }

      List<TransformationFunction> result = new ArrayList<>();
      for (JsonNode funcNode : functionsNode) {
        String json = JSON_MAPPER.writeValueAsString(funcNode);
        TransformationFunction.Builder builder = TransformationFunction.newBuilder();
        JSON_PARSER.merge(json, builder);
        result.add(builder.build());
      }
      return result;
    } catch (Exception e) {
      log.error("Failed to load from {}", filename, e);
      return Collections.emptyList();
    }
  }
}
