package ai.traceable.sensitivedata.config.service;

import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import java.util.Optional;

@lombok.Value
public class ParamTypeRedactionStrategyConfig {
  private static final String REDACTION_STRATEGY_FIELD_NAME = "redactionStrategy";

  RedactionStrategy redactionStrategy;

  public Value toValue() {
    Value redactionStrategyValue =
        Value.newBuilder().setStringValue(redactionStrategy.name()).build();
    Struct struct =
        Struct.newBuilder().putFields(REDACTION_STRATEGY_FIELD_NAME, redactionStrategyValue).build();
    return Value.newBuilder().setStructValue(struct).build();
  }

  public static Optional<ParamTypeRedactionStrategyConfig> fromValue(Value value) {
    if (value == null || value.getKindCase() != KindCase.STRUCT_VALUE) {
      return Optional.empty();
    }
    Value redactionStrategyValue = value.getStructValue().getFieldsMap().get(
        REDACTION_STRATEGY_FIELD_NAME);
    if (redactionStrategyValue == null
        || redactionStrategyValue.getKindCase() != KindCase.STRING_VALUE) {
      return Optional.empty();
    }
    RedactionStrategy redactionStrategy =
        RedactionStrategy.valueOf(redactionStrategyValue.getStringValue());
    return Optional.of(new ParamTypeRedactionStrategyConfig(redactionStrategy));
  }
}
