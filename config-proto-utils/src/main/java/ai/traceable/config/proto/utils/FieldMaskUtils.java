package ai.traceable.config.proto.utils;

import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;

public class FieldMaskUtils {
  // Apply a field mask to a message
  // https://google.aip.dev/134 - update_mask is a standard way to specify which fields to update
  // ignore updates if no update mask is provided.
  // leave it to the caller to use createFieldMaskFromPopulatedFields to generate the mask
  public static <T extends Message> T applyFieldMask(T existing, T update, FieldMask fieldMask) {
    Message.Builder builder = existing.toBuilder();

    if (fieldMask.getPathsList().isEmpty()) {
      fieldMask = generateFieldMask(update);
    }

    for (String path : fieldMask.getPathsList()) {
      applyField(builder, update, path);
    }

    return (T) builder.build();
  }

  // create field mask paths based on a given message
  public static FieldMask generateFieldMask(Message message) {
    List<String> fieldPaths = new ArrayList<>();
    buildFieldPaths(message, "", fieldPaths);
    return FieldMask.newBuilder().addAllPaths(fieldPaths).build();
  }

  private static void buildFieldPaths(Message message, String parentPath, List<String> fieldPaths) {
    Descriptors.Descriptor descriptor = message.getDescriptorForType();

    for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
      Object value = message.getField(field);
      // handle primitive fields.
      if (!field.isRepeated() && !message.hasField(field)) {
        continue;
      }
      if (isEmptyValue(field, value)) {
        continue; // Skip empty fields
      }

      String fieldPath =
          parentPath.isEmpty() ? field.getName() : parentPath + "." + field.getName();
      fieldPaths.add(fieldPath);

      // Recursively process nested message fields
      if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
        if (field.isRepeated()) {
          List<?> repeatedValues = (List<?>) value;
          for (int i = 0; i < repeatedValues.size(); i++) {
            Message nestedMessage = (Message) repeatedValues.get(i);
            if (!isMessageEmpty(nestedMessage)) {
              buildFieldPaths(
                  nestedMessage, fieldPath + "[" + i + "]", fieldPaths); // Use array index
            }
          }
        } else {
          Message nestedMessage = (Message) value;
          if (!isMessageEmpty(nestedMessage)) {
            buildFieldPaths(nestedMessage, fieldPath, fieldPaths);
          }
        }
      }
    }
  }

  private static boolean isEmptyValue(Descriptors.FieldDescriptor field, Object value) {
    if (value == null) return true; // Field is not set
    if (field.isRepeated())
      return ((List<?>) value).isEmpty(); // Check if repeated field (list) is empty
    if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE)
      return isMessageEmpty((Message) value); // Check if message is empty
    if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING)
      return ((String) value).isEmpty(); // Empty string
    if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.BYTE_STRING)
      return ((com.google.protobuf.ByteString) value).isEmpty(); // Empty ByteString
    if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
      return ((Descriptors.EnumValueDescriptor) value).getNumber()
          == 0; // Check if it's the default (first) enum value
    }
    return false; // Other primitive types (int, boolean, etc.) are never "empty"
  }

  private static boolean isMessageEmpty(Message message) {
    return message.getAllFields().isEmpty(); // Checks if the message has any non-default values
  }

  private static void applyField(Message.Builder builder, Message update, String fieldPath) {
    String[] parts = fieldPath.split("\\.");
    applyNestedField(builder, update, parts, 0);
  }

  private static void applyNestedField(
      Message.Builder builder, Message update, String[] parts, int index) {
    if (index >= parts.length) return;

    String fieldName = parts[index];
    Descriptors.FieldDescriptor fieldDescriptor =
        builder.getDescriptorForType().findFieldByName(fieldName);
    if (fieldDescriptor == null) return;

    if (index == parts.length - 1) {
      // Base case: update the actual field
      builder.setField(fieldDescriptor, update.getField(fieldDescriptor));
    } else {
      // Recursive case: navigate into nested fields
      Message.Builder nestedBuilder = ((Message) builder.getField(fieldDescriptor)).toBuilder();
      applyNestedField(nestedBuilder, (Message) update.getField(fieldDescriptor), parts, index + 1);
      builder.setField(fieldDescriptor, nestedBuilder.build());
    }
  }
}
