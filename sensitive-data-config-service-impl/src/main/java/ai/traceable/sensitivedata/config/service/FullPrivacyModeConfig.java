package ai.traceable.sensitivedata.config.service;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import java.util.Optional;

@lombok.Value
public class FullPrivacyModeConfig {
  private static final String ENABLED_FIELD_NAME = "enabled";

  boolean enabled;

  public Value toValue() {
    Value fullPrivacyModeValue = Value.newBuilder().setBoolValue(enabled).build();
    Struct struct = Struct.newBuilder().putFields(ENABLED_FIELD_NAME, fullPrivacyModeValue).build();
    return Value.newBuilder().setStructValue(struct).build();
  }

  public static Optional<FullPrivacyModeConfig> fromValue(Value value) {
    if (value == null || value.getKindCase() != KindCase.STRUCT_VALUE) {
      return Optional.empty();
    }
    Value enabledValue = value.getStructValue().getFieldsMap().get(ENABLED_FIELD_NAME);
    if (enabledValue == null || enabledValue.getKindCase() != KindCase.BOOL_VALUE) {
      return Optional.empty();
    }
    return Optional.of(new FullPrivacyModeConfig(enabledValue.getBoolValue()));
  }
}
