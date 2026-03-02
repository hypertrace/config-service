package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.util.JsonFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class YamlFileReader {

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  private YamlFileReader() {}

  public static List<EntityDerivationConfig> readEntitiesFromYaml(String resourceFile) {
    try {
      JsonNode root =
          YAML_MAPPER.readValue(
              YamlFileReader.class.getClassLoader().getResourceAsStream(resourceFile),
              JsonNode.class);
      JsonNode entitiesNode = root.get("entities");

      if (entitiesNode == null || !entitiesNode.isArray()) {
        log.error("Invalid YAML structure in {}: missing 'entities' array", resourceFile);
        return Collections.emptyList();
      }

      List<EntityDerivationConfig> result = new ArrayList<>();
      for (JsonNode entityNode : entitiesNode) {
        String json = JSON_MAPPER.writeValueAsString(entityNode);
        EntityDerivationConfig.Builder builder = EntityDerivationConfig.newBuilder();
        JSON_PARSER.merge(json, builder);
        result.add(builder.build());
      }
      log.info("Loaded {} entity derivations from {}", result.size(), resourceFile);
      return result;
    } catch (Exception e) {
      log.error("Failed to load entity derivations from {}", resourceFile, e);
      return Collections.emptyList();
    }
  }
}
