package ai.traceable.anomaly.config.service.common;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.List;

public final class AnomalyConfigServiceUtils {
  public static Message mergeConfigs(Message fallbackConfig, Message preferredConfig) {
    Message.Builder builder = fallbackConfig.toBuilder();
    AnomalyConfigServiceUtils.mergeFromProto(builder, preferredConfig);
    return builder.build();
  }

  private static void mergeFromProto(Message.Builder builder, Message source) {
    List<Descriptors.FieldDescriptor> fieldDescriptors = builder.getDescriptorForType().getFields();
    for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
      if (fieldDescriptor.isRepeated()) {
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
