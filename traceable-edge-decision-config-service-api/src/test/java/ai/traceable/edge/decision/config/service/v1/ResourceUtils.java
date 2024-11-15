package ai.traceable.edge.decision.engine.config.v1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.io.Resources;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class ResourceUtils {
  private static final JsonFormat.Printer jsonPrinter =
      JsonFormat.printer().includingDefaultValueFields();
  private static final JsonFormat.Parser jsonParser = JsonFormat.parser();
  private static final ObjectMapper objectMapper = new ObjectMapper();
  public static final YAMLMapper YAML_MAPPER = new YAMLMapper();

  public static List<String> getConfigs(List<String> files) throws IOException {
    List<String> configs = new ArrayList<>(files.size());
    for (String configFile : files) {
      URL resource = Resources.getResource(configFile);
      String yamlConfig = Resources.toString(resource, Charset.defaultCharset());
      configs.add(yamlConfig);
    }
    return configs;
  }

  public static String getConfig(String file) throws IOException {
    URL resource = Resources.getResource(file);
    return Resources.toString(resource, Charset.defaultCharset());
  }

  public static <T extends Message.Builder> T readProto(String resourceName, T builder)
      throws IOException {
    URL url = Resources.getResource(resourceName);
    String data = Resources.toString(url, Charset.defaultCharset());
    return deserialize(data, builder);
  }

  public static <T extends Message.Builder> T readProtoFromYaml(String resourceName, T builder)
      throws IOException {
    URL url = Resources.getResource(resourceName);
    String data = Resources.toString(url, Charset.defaultCharset());
    return deserializeYaml(data, builder);
  }

  public static <T extends Message.Builder> T deserializeYaml(String value, T builder)
      throws JsonProcessingException {
    try {
      var json = YAML_MAPPER.readValue(value, JsonNode.class);
      jsonParser.merge(json.toString(), builder);
      return builder;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends Message.Builder> T deserialize(String value, T builder)
      throws JsonProcessingException {
    try {
      jsonParser.merge(value, builder);
      return builder;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
