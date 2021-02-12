package ai.traceable.sensitivedata.config.service;

import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import lombok.AllArgsConstructor;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

@lombok.Value
@AllArgsConstructor
public class RedactionRuleConfig implements Comparable<RedactionRuleConfig> {
  private static final String REDACTION_RULE_FIELD_NAME = "redactionRule";
  private static final String CREATION_TIMESTAMP_FIELD_NAME = "creationTimestamp";

  RedactionRule redactionRule;
  long creationTimestamp;

  public RedactionRuleConfig(RedactionRule redactionRule) {
    this(redactionRule, System.currentTimeMillis());
  }

  public Value toValue() {
    Value redactionRuleValue;
    try {
      redactionRuleValue = ConfigProtoConverter.convertToValue(redactionRule);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    Value creationTimestampValue = Value.newBuilder().setNumberValue(creationTimestamp).build();
    Struct struct =
        Struct.newBuilder()
            .putFields(REDACTION_RULE_FIELD_NAME, redactionRuleValue)
            .putFields(CREATION_TIMESTAMP_FIELD_NAME, creationTimestampValue)
            .build();
    return Value.newBuilder().setStructValue(struct).build();
  }

  public static RedactionRuleConfig fromValue(Value value) {
    Preconditions.checkArgument(value != null && value.getKindCase() == KindCase.STRUCT_VALUE);
    Value redactionRuleValue = value.getStructValue().getFieldsMap().get(REDACTION_RULE_FIELD_NAME);
    Value creationTimestampValue =
        value.getStructValue().getFieldsMap().get(CREATION_TIMESTAMP_FIELD_NAME);
    Preconditions.checkArgument(
        redactionRuleValue != null
            && redactionRuleValue.getKindCase() == KindCase.STRUCT_VALUE
            && creationTimestampValue != null
            && creationTimestampValue.getKindCase() == KindCase.NUMBER_VALUE);
    RedactionRule.Builder redactionRuleBuilder = RedactionRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(redactionRuleValue, redactionRuleBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    long creationTimestamp = (long) creationTimestampValue.getNumberValue();
    return new RedactionRuleConfig(redactionRuleBuilder.build(), creationTimestamp);
  }

  @Override
  public int compareTo(RedactionRuleConfig anotherRedactionRuleConfig) {
    return Long.compare(
        this.getCreationTimestamp(), anotherRedactionRuleConfig.getCreationTimestamp());
  }
}
