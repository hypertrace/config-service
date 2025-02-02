package ai.traceable.config.proto.utils;

import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;

public class FieldMaskUtils {
  // Apply a field mask to a message
  // https://google.aip.dev/134 - update_mask is a standard way to specify which fields to update
  // ignore updates if no update mask is provided.
  // leave it to the caller to use createFieldMaskFromPopulatedFields to generate the mask
  public static <T extends Message> T applyFieldMask(T existing, T update, FieldMask fieldMask) {
    Message.Builder builder = existing.toBuilder();

    for (String path : fieldMask.getPathsList()) {
      applyField(builder, update, path);
    }

    return (T) builder.build();
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
