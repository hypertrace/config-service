package org.hypertrace.config.service.test;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.LinkedHashMap;
import java.util.Map;

/** Duplicated from config service impl to mock correct behavior without publishing impl */
class ConfigServiceUtils {

  private ConfigServiceUtils() {
    // to prevent instantiation
  }

  /**
   * Deep merge the specified {@link Value} configs with overridingConfig taking precedence over
   * defaultConfig for the same keys.
   *
   * @param defaultConfig
   * @param overridingConfig
   * @return the resulting config obtained after merging defaultConfig and overridingConfig
   */
  static Value merge(Value defaultConfig, Value overridingConfig) {
    if (isNull(defaultConfig)) {
      return overridingConfig;
    }
    if (isNull(overridingConfig)) {
      return defaultConfig;
    }

    // Only if both - defaultConfig and overridingConfig are of kind Struct(Map), then merge
    // the common fields. Otherwise, just return the overridingConfig
    if (defaultConfig.getKindCase() == Value.KindCase.STRUCT_VALUE
        && overridingConfig.getKindCase() == Value.KindCase.STRUCT_VALUE) {
      Map<String, Value> defaultConfigMap = defaultConfig.getStructValue().getFieldsMap();
      Map<String, Value> overridingConfigMap = overridingConfig.getStructValue().getFieldsMap();

      Map<String, Value> resultConfigMap = new LinkedHashMap<>(defaultConfigMap);
      for (Map.Entry<String, Value> entry : overridingConfigMap.entrySet()) {
        resultConfigMap.put(
            entry.getKey(), merge(defaultConfigMap.get(entry.getKey()), entry.getValue()));
      }
      Struct struct = Struct.newBuilder().putAllFields(resultConfigMap).build();
      return Value.newBuilder().setStructValue(struct).build();
    }
    return overridingConfig;
  }

  private static boolean isNull(Value value) {
    if (value == null) {
      return true;
    }
    return value.getKindCase() == Value.KindCase.NULL_VALUE;
  }
}
