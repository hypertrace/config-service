package ai.traceable.cloud.edge.deployment.config.service.v1.shared.config;

import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigValueDescriptor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.util.JsonFormat;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class SharedConfigMetadataParser {
  private static final ObjectMapper YAML_OBJECT_MAPPER =
      new ObjectMapper(new YAMLFactory())
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  public List<ConfigValueDescriptor> loadConfigValueDescriptors(String fileName) {
    try {
      JsonNode rootNode =
          YAML_OBJECT_MAPPER.readValue(
              this.getClass().getClassLoader().getResourceAsStream(fileName), JsonNode.class);

      List<ConfigValueDescriptor> result = new ArrayList<>();

      for (JsonNode node : rootNode) {
        String json = JSON_OBJECT_MAPPER.writeValueAsString(node);
        ConfigValueDescriptor.Builder builder = ConfigValueDescriptor.newBuilder();
        JSON_PARSER.merge(json, builder);
        result.add(builder.build());
      }

      return result;

    } catch (Exception e) {
      log.error("Failed to load config descriptors from {}", fileName, e);
      return Collections.emptyList();
    }
  }
}
