package ai.traceable.anomaly.config.service.common;

import com.google.protobuf.Descriptors;
import com.google.protobuf.MapEntry;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class AnomalyConfigServiceUtils {

  public static Message mergeConfigs(Message fallbackConfig, Message preferredConfig) {
    Message.Builder builder = fallbackConfig.toBuilder();
    AnomalyConfigServiceUtils.mergeFromProto(builder, preferredConfig);
    return builder.build();
  }

  private static void mergeFromProto(Message.Builder builder, Message source) {
    List<Descriptors.FieldDescriptor> fieldDescriptors = builder.getDescriptorForType().getFields();
    for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
      if (fieldDescriptor.isMapField()) {
        Map<Object, Object> mergedEntries =
            ((List<MapEntry<Object, Object>>) builder.getField(fieldDescriptor))
                .stream().collect(Collectors.toMap(MapEntry::getKey, MapEntry::getValue));
        ((List<MapEntry<Object, Object>>) source.getField(fieldDescriptor))
            .forEach(
                entry -> {
                  Message fallback = (Message) mergedEntries.get(entry.getKey());
                  Message merged =
                      fallback != null
                          ? mergeConfigs(fallback, (Message) entry.getValue())
                          : (Message) entry.getValue();
                  mergedEntries.put(entry.getKey(), merged);
                });
        List<MapEntry> mergedField = new ArrayList<>();
        mergedEntries.forEach(
            (k, v) -> {
              MapEntry.Builder entryBuilder =
                  (MapEntry.Builder) builder.newBuilderForField(fieldDescriptor);
              entryBuilder.setKey(k);
              entryBuilder.setValue(v);
              mergedField.add(entryBuilder.build());
            });
        builder.setField(fieldDescriptor, mergedField);
      } else if (fieldDescriptor.isRepeated()) {
        builder.setField(fieldDescriptor, source.getField(fieldDescriptor));
      } else if (source.hasField(fieldDescriptor)) {
        if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
          Message.Builder childBuilder = ((Message) builder.getField(fieldDescriptor)).toBuilder();
          mergeFromProto(childBuilder, (Message) source.getField(fieldDescriptor));
          builder.setField(fieldDescriptor, childBuilder.build());
        } else {
          builder.setField(fieldDescriptor, source.getField(fieldDescriptor));
        }
      }
    }
  }
}
