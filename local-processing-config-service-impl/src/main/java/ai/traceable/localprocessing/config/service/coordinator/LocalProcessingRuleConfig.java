package ai.traceable.localprocessing.config.service.coordinator;

import ai.traceable.localprocessing.config.service.v1.LocalProcessingRule;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import lombok.AllArgsConstructor;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

@lombok.Value
@AllArgsConstructor
public class LocalProcessingRuleConfig implements Comparable<LocalProcessingRuleConfig> {
  private static final String LOCAL_PROCESSING_RULE_FIELD_NAME = "localProcessingRule";
  private static final String CREATION_TIMESTAMP_FIELD_NAME = "creationTimestamp";

  LocalProcessingRule localProcessingRule;
  long creationTimestamp;

  public LocalProcessingRuleConfig(LocalProcessingRule localProcessingRule) {
    this(localProcessingRule, System.currentTimeMillis());
  }

  public Value toValue() {
    Value localProcessingRuleValue;
    try {
      localProcessingRuleValue = ConfigProtoConverter.convertToValue(localProcessingRule);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    Value creationTimestampValue = Value.newBuilder().setNumberValue(creationTimestamp).build();
    Struct struct =
        Struct.newBuilder()
            .putFields(LOCAL_PROCESSING_RULE_FIELD_NAME, localProcessingRuleValue)
            .putFields(CREATION_TIMESTAMP_FIELD_NAME, creationTimestampValue)
            .build();
    return Value.newBuilder().setStructValue(struct).build();
  }

  public static LocalProcessingRuleConfig fromValue(Value value) {
    Preconditions.checkArgument(
        value != null && value.getKindCase() == Value.KindCase.STRUCT_VALUE);
    Value localProcessingRuleValue =
        value.getStructValue().getFieldsMap().get(LOCAL_PROCESSING_RULE_FIELD_NAME);
    Value creationTimestampValue =
        value.getStructValue().getFieldsMap().get(CREATION_TIMESTAMP_FIELD_NAME);
    Preconditions.checkArgument(
        localProcessingRuleValue != null
            && localProcessingRuleValue.getKindCase() == Value.KindCase.STRUCT_VALUE
            && creationTimestampValue != null
            && creationTimestampValue.getKindCase() == Value.KindCase.NUMBER_VALUE);
    LocalProcessingRule.Builder redactionRuleBuilder = LocalProcessingRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(localProcessingRuleValue, redactionRuleBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    long creationTimestamp = (long) creationTimestampValue.getNumberValue();
    return new LocalProcessingRuleConfig(redactionRuleBuilder.build(), creationTimestamp);
  }

  @Override
  public int compareTo(LocalProcessingRuleConfig anotherLocalProcessingRuleConfig) {
    return Long.compare(
        this.getCreationTimestamp(), anotherLocalProcessingRuleConfig.getCreationTimestamp());
  }
}
