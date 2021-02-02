package ai.traceable.sensitivedata.config.service;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import java.util.Optional;

@lombok.Value
public class AutomaticSecretRedactionStrategyConfig {
  private static final String ENABLED_FIELD_NAME = "enabled";

  boolean enabled;

  public Value toValue() {
    Value redactionStrategyValue =
        Value.newBuilder().setBoolValue(enabled).build();
    Struct struct =
        Struct.newBuilder().putFields(ENABLED_FIELD_NAME, redactionStrategyValue).build();
    return Value.newBuilder().setStructValue(struct).build();
  }

  public static Optional<AutomaticSecretRedactionStrategyConfig> fromValue(Value value) {
    if (value == null || value.getKindCase() != KindCase.STRUCT_VALUE) {
      return Optional.empty();
    }
    Value enabledValue = value.getStructValue().getFieldsMap().get(ENABLED_FIELD_NAME);
    if (enabledValue == null
        || enabledValue.getKindCase() != KindCase.BOOL_VALUE) {
      return Optional.empty();
    }
    return Optional.of(new AutomaticSecretRedactionStrategyConfig(enabledValue.getBoolValue()));
  }
}
