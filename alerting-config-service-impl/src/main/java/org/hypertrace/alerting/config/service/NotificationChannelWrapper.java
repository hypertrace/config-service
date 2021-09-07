package org.hypertrace.alerting.config.service;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import lombok.AllArgsConstructor;
import org.hypertrace.alerting.config.service.v1.NotificationChannel;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

@lombok.Value
@AllArgsConstructor
public class NotificationChannelWrapper implements Comparable<NotificationChannelWrapper> {
  private static final String NOTIFICATION_CHANNEL_FIELD_NAME = "NotificationChannel";
  private static final String CREATION_TIMESTAMP_FIELD_NAME = "creationTimestamp";

  org.hypertrace.alerting.config.service.v1.NotificationChannel NotificationChannel;
  long creationTimestamp;

  public NotificationChannelWrapper(NotificationChannel notificationChannel) {
    this(notificationChannel, System.currentTimeMillis());
  }

  public Value toValue() {
    Value notificationChannelValue;
    try {
      notificationChannelValue = ConfigProtoConverter.convertToValue(NotificationChannel);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    Value creationTimestampValue = Value.newBuilder().setNumberValue(creationTimestamp).build();
    Struct struct =
        Struct.newBuilder()
            .putFields(NOTIFICATION_CHANNEL_FIELD_NAME, notificationChannelValue)
            .putFields(CREATION_TIMESTAMP_FIELD_NAME, creationTimestampValue)
            .build();
    return Value.newBuilder().setStructValue(struct).build();
  }

  public static NotificationChannelWrapper fromValue(Value value) {
    Preconditions.checkArgument(
        value != null && value.getKindCase() == Value.KindCase.STRUCT_VALUE);
    Value notificationChannelValue =
        value.getStructValue().getFieldsMap().get(NOTIFICATION_CHANNEL_FIELD_NAME);
    Value creationTimestampValue =
        value.getStructValue().getFieldsMap().get(CREATION_TIMESTAMP_FIELD_NAME);
    Preconditions.checkArgument(
        notificationChannelValue != null
            && notificationChannelValue.getKindCase() == Value.KindCase.STRUCT_VALUE
            && creationTimestampValue != null
            && creationTimestampValue.getKindCase() == Value.KindCase.NUMBER_VALUE);
    NotificationChannel.Builder notificationChannelBuilder =
        org.hypertrace.alerting.config.service.v1.NotificationChannel.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(notificationChannelValue, notificationChannelBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    long creationTimestamp = (long) creationTimestampValue.getNumberValue();
    return new NotificationChannelWrapper(notificationChannelBuilder.build(), creationTimestamp);
  }

  @Override
  public int compareTo(NotificationChannelWrapper anotherNotificationChannelWrapper) {
    return Long.compare(
        anotherNotificationChannelWrapper.getCreationTimestamp(), this.getCreationTimestamp());
  }
}
