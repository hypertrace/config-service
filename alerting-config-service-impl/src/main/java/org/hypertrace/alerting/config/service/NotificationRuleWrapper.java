package org.hypertrace.alerting.config.service;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import lombok.AllArgsConstructor;
import org.hypertrace.alerting.config.service.v1.NotificationRule;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

@lombok.Value
@AllArgsConstructor
public class NotificationRuleWrapper implements Comparable<NotificationRuleWrapper> {
  private static final String NOTIFICATION_RULE_FIELD_NAME = "NotificationRule";
  private static final String CREATION_TIMESTAMP_FIELD_NAME = "creationTimestamp";

  NotificationRule notificationRule;
  long creationTimestamp;

  public NotificationRuleWrapper(NotificationRule notificationRule) {
    this(notificationRule, System.currentTimeMillis());
  }

  public Value toValue() {
    Value notificationRuleValue;
    try {
      notificationRuleValue = ConfigProtoConverter.convertToValue(notificationRule);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    Value creationTimestampValue = Value.newBuilder().setNumberValue(creationTimestamp).build();
    Struct struct =
        Struct.newBuilder()
            .putFields(NOTIFICATION_RULE_FIELD_NAME, notificationRuleValue)
            .putFields(CREATION_TIMESTAMP_FIELD_NAME, creationTimestampValue)
            .build();
    return Value.newBuilder().setStructValue(struct).build();
  }

  public static NotificationRuleWrapper fromValue(Value value) {
    Preconditions.checkArgument(
        value != null && value.getKindCase() == Value.KindCase.STRUCT_VALUE);
    Value notificationRuleValue =
        value.getStructValue().getFieldsMap().get(NOTIFICATION_RULE_FIELD_NAME);
    Value creationTimestampValue =
        value.getStructValue().getFieldsMap().get(CREATION_TIMESTAMP_FIELD_NAME);
    Preconditions.checkArgument(
        notificationRuleValue != null
            && notificationRuleValue.getKindCase() == Value.KindCase.STRUCT_VALUE
            && creationTimestampValue != null
            && creationTimestampValue.getKindCase() == Value.KindCase.NUMBER_VALUE);
    NotificationRule.Builder notificationRuleBuilder = NotificationRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(notificationRuleValue, notificationRuleBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    long creationTimestamp = (long) creationTimestampValue.getNumberValue();
    return new NotificationRuleWrapper(notificationRuleBuilder.build(), creationTimestamp);
  }

  @Override
  public int compareTo(NotificationRuleWrapper anotherNotificationRuleWrapper) {
    return Long.compare(
        anotherNotificationRuleWrapper.getCreationTimestamp(), this.getCreationTimestamp());
  }
}
