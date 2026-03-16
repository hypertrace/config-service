package ai.traceable.fraud.datamodel.event.kind.operator;

import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.Operator;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorsByKind;
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

/** Default implementation that loads operators from YAML resource file. */
@Slf4j
@Singleton
public class DefaultOperatorProvider implements OperatorProvider {

  private static final String RESOURCE_FILE = "operators.yaml";
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  private final List<Operator> operators;
  private final EventKindHierarchyResolver hierarchyResolver;

  @Inject
  public DefaultOperatorProvider(EventKindHierarchyResolver hierarchyResolver) {
    this.hierarchyResolver = hierarchyResolver;
    this.operators = loadOperators();
  }

  @Override
  public List<OperatorsByKind> getOperatorsByKinds(List<ComplexDataModelEventKind> requestedKinds) {
    List<OperatorsByKind> result = new ArrayList<>();

    for (ComplexDataModelEventKind kind : requestedKinds) {
      List<Operator> applicableOperators =
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

  @Override
  public List<Operator> getAllOperators() {
    return operators;
  }

  private boolean isCompatibleWithKind(Operator operator, ComplexDataModelEventKind requestedKind) {
    for (ComplexDataModelEventKind inputKind : operator.getInputKindsList()) {
      if (hierarchyResolver.isCompatible(inputKind, requestedKind)) {
        return true;
      }
    }
    return false;
  }

  private List<Operator> loadOperators() {
    try {
      InputStream stream = getClass().getClassLoader().getResourceAsStream(RESOURCE_FILE);
      if (stream == null) {
        log.warn("Resource file not found: {}", RESOURCE_FILE);
        return Collections.emptyList();
      }

      JsonNode root = YAML_MAPPER.readValue(stream, JsonNode.class);
      JsonNode functionsNode = root.get("functions");

      if (functionsNode == null || !functionsNode.isArray()) {
        log.error("Invalid YAML structure in {}: missing 'functions' array", RESOURCE_FILE);
        return Collections.emptyList();
      }

      List<Operator> result = new ArrayList<>();
      for (JsonNode funcNode : functionsNode) {
        String json = JSON_MAPPER.writeValueAsString(funcNode);
        Operator.Builder builder = Operator.newBuilder();
        JSON_PARSER.merge(json, builder);
        result.add(builder.build());
      }
      return Collections.unmodifiableList(result);
    } catch (Exception e) {
      log.error("Failed to load operators from {}", RESOURCE_FILE, e);
      return Collections.emptyList();
    }
  }
}
