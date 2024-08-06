package ai.traceable.fraud.datamodel.derivation.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.traceable.fraud.datamodel.derivation.config.service.v1.UserAgentMergeMappingConfig;
import com.google.common.io.Resources;
import com.google.protobuf.ListValue;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UserAgentMergeMappingConfigTest {
  private Map<String, List<String>> map;

  @BeforeEach
  public void setUp() throws Exception {
    // JSON file path
    URL url = Resources.getResource("user_agent_merge_mapping.json");
    String jsonFilePath = url.getPath();
    // Create a FileReader
    try (Reader reader = new FileReader(jsonFilePath)) {
      // Create a builder for your Protocol Buffer message
      UserAgentMergeMappingConfig.Builder builder = UserAgentMergeMappingConfig.newBuilder();
      Struct struct = (Struct) fromJson(reader);

      // Convert Struct to Map<String, List<String>>
      map = convertStructToMap(struct);
      printMap(map);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testConvertToConfig() {
    UserAgentMergeMappingConfig config = convertToUserAgentMergeMappingConfig(map);
    assertNotNull(config);
  }

  @Test
  public void testConvertToMap() {
    UserAgentMergeMappingConfig config = convertToUserAgentMergeMappingConfig(map);
    Map<String, List<String>> actual = convertToMap(config);
    assertEquals(map, actual);
  }

  private void printMap(Map<String, List<String>> map) {
    for (Map.Entry<String, List<String>> entry : map.entrySet()) {
      System.out.println("Key: " + entry.getKey());
      System.out.println("Values:");
      for (String value : entry.getValue()) {
        System.out.println("- " + value);
      }
    }
  }

  private List<String> convertListValueToList(ListValue listValue) {
    return listValue.getValuesList().stream()
        .map(Value::getStringValue)
        .collect(Collectors.toList());
  }

  public Message fromJson(Reader reader) throws IOException {
    Message.Builder structBuilder = Struct.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(reader, structBuilder);
    return structBuilder.build();
  }

  public Map<String, List<String>> convertStructToMap(Struct struct) {
    Map<String, List<String>> map = new HashMap<>();
    for (Map.Entry<String, Value> entry : struct.getFieldsMap().entrySet()) {
      List<String> values =
          entry.getValue().getListValue().getValuesList().stream()
              .map(Value::getStringValue)
              .collect(Collectors.toList());
      map.put(entry.getKey(), values);
    }
    return map;
  }

  public UserAgentMergeMappingConfig convertToUserAgentMergeMappingConfig(
      Map<String, List<String>> map) {
    UserAgentMergeMappingConfig.Builder builder = UserAgentMergeMappingConfig.newBuilder();

    // Iterate over the map entries and convert each value to ListValue
    for (Map.Entry<String, List<String>> entry : map.entrySet()) {
      String key = entry.getKey();
      List<String> values = entry.getValue();

      // Convert List<String> to ListValue
      ListValue.Builder listValueBuilder = ListValue.newBuilder();
      listValueBuilder.addAllValues(
          values.stream()
              .map(value -> Value.newBuilder().setStringValue(value).build())
              .collect(Collectors.toList()));

      // Add to pairs map in the protobuf message
      builder.putPairs(key, listValueBuilder.build());
    }

    // Build and return the protobuf message
    return builder.build();
  }

  public Map<String, List<String>> convertToMap(UserAgentMergeMappingConfig config) {
    Map<String, List<String>> map = new HashMap<>();

    // Iterate over the pairs map in the protobuf message
    for (Map.Entry<String, ListValue> entry : config.getPairsMap().entrySet()) {
      String key = entry.getKey();
      ListValue listValue = entry.getValue();

      // Convert ListValue to List<String>
      List<String> values =
          listValue.getValuesList().stream()
              .map(Value::getStringValue)
              .collect(Collectors.toList());

      // Put the entry into the map
      map.put(key, values);
    }

    return map;
  }
}
