package org.hypertrace.config.service;

import static org.hypertrace.config.service.store.ConfigDocument.CONFIG_FIELD_NAME;

import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.v1.ContextSpecificConfig;

/** This class contains utility methods. */
@Slf4j
public class ConfigServiceUtils {

  private static final Value EMPTY_VALUE =
      Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();

  private ConfigServiceUtils() {
    // to prevent instantiation
  }

  /**
   * Deep merge the specified {@link Value} configs with overridingConfig taking precedence over
   * defaultConfig for the same keys.
   *
   * @param defaultContextSpecificConfig
   * @param overridingContextSpecificConfig
   * @return the resulting config obtained after merging defaultConfig and overridingConfig
   */
  public static ContextSpecificConfig merge(
      ContextSpecificConfig defaultContextSpecificConfig,
      ContextSpecificConfig overridingContextSpecificConfig) {

    Value defaultConfig = defaultContextSpecificConfig.getConfig();
    Value overridingConfig = overridingContextSpecificConfig.getConfig();
    if (isNull(defaultConfig)) {
      return overridingContextSpecificConfig;
    } else if (isNull(overridingConfig)) {
      return defaultContextSpecificConfig;
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
            entry.getKey(),
            merge(
                    defaultConfigMap.containsKey(entry.getKey())
                        ? buildContextSpecificConfig(
                            defaultConfigMap.get(entry.getKey()),
                            defaultContextSpecificConfig.getCreationTimestamp(),
                            defaultContextSpecificConfig.getUpdateTimestamp())
                        : buildContextSpecificConfig(
                            Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build(),
                            defaultContextSpecificConfig.getCreationTimestamp(),
                            defaultContextSpecificConfig.getUpdateTimestamp()),
                    buildContextSpecificConfig(
                        entry.getValue(),
                        overridingContextSpecificConfig.getCreationTimestamp(),
                        overridingContextSpecificConfig.getUpdateTimestamp()))
                .getConfig());
      }
      Struct struct = Struct.newBuilder().putAllFields(resultConfigMap).build();
      return ContextSpecificConfig.newBuilder()
          .setConfig(Value.newBuilder().setStructValue(struct).build())
          .setCreationTimestamp(overridingContextSpecificConfig.getCreationTimestamp())
          .setUpdateTimestamp(overridingContextSpecificConfig.getUpdateTimestamp())
          .build();
    } else {
      return overridingContextSpecificConfig;
    }
  }

  public static boolean isNull(Value value) {
    if (value == null) {
      log.error(
          "NULL Value encountered. This is unexpected and indicates a BUG in code.",
          new RuntimeException());
      return true;
    }
    return value.getKindCase() == Value.KindCase.NULL_VALUE;
  }

  public static Optional<ContextSpecificConfig> filterNull(
      ContextSpecificConfig contextSpecificConfig) {
    return isNull(contextSpecificConfig.getConfig())
        ? Optional.empty()
        : Optional.of(contextSpecificConfig);
  }

  public static Value emptyValue() {
    return EMPTY_VALUE;
  }

  public static ContextSpecificConfig emptyConfig(String context) {
    return ContextSpecificConfig.newBuilder()
        .setConfig(EMPTY_VALUE)
        .setContext(context)
        .setCreationTimestamp(0)
        .setUpdateTimestamp(0)
        .build();
  }

  public static String buildConfigFieldPath(String configJsonPath) {
    return String.format("%s.%s", CONFIG_FIELD_NAME, configJsonPath);
  }

  private static ContextSpecificConfig buildContextSpecificConfig(
      Value config, long creationTimestamp, long updateTimestamp) {
    return ContextSpecificConfig.newBuilder()
        .setConfig(config)
        .setCreationTimestamp(creationTimestamp)
        .setUpdateTimestamp(updateTimestamp)
        .build();
  }
}
