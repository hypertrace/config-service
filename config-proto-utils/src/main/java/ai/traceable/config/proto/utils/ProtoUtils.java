package ai.traceable.config.proto.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtoUtils {
  private static final JsonFormat.Printer jsonPrinter =
      JsonFormat.printer().includingDefaultValueFields();
  private static final JsonFormat.Parser jsonParser = JsonFormat.parser();
  private static final ObjectMapper objectMapper = new ObjectMapper();
  public static final YAMLMapper YAML_MAPPER = new YAMLMapper();

  public static void print(Message m) {
    System.out.println(serializeWithAllFields(m));
  }

  public static String serialize(Message m) {
    try {
      return jsonPrinter.preservingProtoFieldNames().print(m);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static String serializeNoNewlines(Message m) {
    try {
      return jsonPrinter.preservingProtoFieldNames().omittingInsignificantWhitespace().print(m);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static String serializeNoPreserve(Message m) {
    try {
      return jsonPrinter.print(m);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static String serializeWithAllFields(Message m) {
    try {
      return jsonPrinter.preservingProtoFieldNames().print(m);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static Map<String, Object> serializeToMap(Message m) throws JsonProcessingException {
    return objectMapper.readValue(serializeWithAllFields(m), Map.class);
  }

  public static <T extends Message.Builder> T deserialize(String value, T builder) {
    try {
      jsonParser.merge(value, builder);
      return builder;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends Message.Builder> T deserializeYaml(String value, T builder)
      throws JsonProcessingException {
    try {
      JsonNode json = YAML_MAPPER.readValue(value, JsonNode.class);
      jsonParser.merge(json.toString(), builder);
      return builder;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> Message.Builder deserializeNoError(String value, Message.Builder builder) {
    try {
      jsonParser.merge(value, builder);
      return builder;
    } catch (InvalidProtocolBufferException e) {
      log.error("Error deserializing " + value + " for builder: " + builder.getClass().getName());
      return builder;
    }
  }

  public static <T extends Message.Builder> T deserialize(byte[] bytes, T builder) {
    try {
      jsonParser.merge(new InputStreamReader(new ByteArrayInputStream(bytes)), builder);
      return builder;
    } catch (IOException e) {
      throw new RuntimeException(
          "Error while parsing bytes input to " + builder.getDescriptorForType().getName(), e);
    }
  }

  public static <T extends Message.Builder> T deserializeNoError(byte[] bytes, T builder) {
    try {
      jsonParser
          .ignoringUnknownFields()
          .merge(new InputStreamReader(new ByteArrayInputStream(bytes)), builder);
      return builder;
    } catch (IOException e) {
      log.error(
          "Error while parsing bytes input to " + builder.getDescriptorForType().getName(), e);
      return builder;
    }
  }

  public static <T extends Message> List<T> find(Message message, Class<T> targetType) {
    List<T> matches = new ArrayList<>();
    find(message, targetType, matches);
    return matches;
  }

  private static <T extends Message> void find(
      Message message, Class<T> targetType, List<T> matches) {
    if (targetType.isInstance(message)) {
      matches.add(targetType.cast(message)); // Found a match, add to list
    }

    for (Descriptors.FieldDescriptor field : message.getDescriptorForType().getFields()) {
      if (!field.isRepeated() && !message.hasField(field)) {
        continue; // Skip missing non-repeated fields
      }

      Object value = message.getField(field);

      if (field.isMapField()) { // Handle Protobuf map fields (stored as lists of entry messages)
        List<?> entryList = (List<?>) value;
        for (Object entry : entryList) {
          if (entry instanceof Message) {
            Message entryMessage = (Message) entry;
            Descriptors.FieldDescriptor valueField =
                entryMessage.getDescriptorForType().findFieldByName("value");
            if (valueField != null && entryMessage.hasField(valueField)) {
              Object mapValue = entryMessage.getField(valueField);
              if (mapValue instanceof Message) {
                find((Message) mapValue, targetType, matches);
              }
            }
          }
        }
      } else if (field.isRepeated()) { // Handle repeated fields (lists of messages)
        for (Object item : (List<?>) value) {
          if (item instanceof Message) {
            find((Message) item, targetType, matches);
          }
        }
      } else if (value instanceof Message) { // Handle single sub-message
        find((Message) value, targetType, matches);
      }
    }
  }
}
