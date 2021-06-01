package ai.traceable.localprocessing.config.service;

import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.Optional;

@lombok.Value
public class DefaultProtectionModeConfigConverter {
  private static final String DEFAULT_PROTECTION_MODE_FIELD_NAME = "defaultProtectionMode";

  public static Value toValue(ProtectionMode defaultProtectionMode) {
    Value defaultProtectionModeValue =
        Value.newBuilder().setStringValue(defaultProtectionMode.name()).build();
    Struct struct =
        Struct.newBuilder()
            .putFields(DEFAULT_PROTECTION_MODE_FIELD_NAME, defaultProtectionModeValue)
            .build();
    return Value.newBuilder().setStructValue(struct).build();
  }

  public static Optional<ProtectionMode> fromValue(Value value) {
    if (value == null || value.getKindCase() != Value.KindCase.STRUCT_VALUE) {
      return Optional.empty();
    }
    Value defaultProtectionModeValue =
        value.getStructValue().getFieldsMap().get(DEFAULT_PROTECTION_MODE_FIELD_NAME);
    if (defaultProtectionModeValue == null
        || defaultProtectionModeValue.getKindCase() != Value.KindCase.STRING_VALUE) {
      return Optional.empty();
    }
    return Optional.of(ProtectionMode.valueOf(defaultProtectionModeValue.getStringValue()));
  }
}
