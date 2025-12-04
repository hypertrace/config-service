package org.hypertrace.config.objectstore;

import com.google.protobuf.Value;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.UpsertConfigResponse;

@lombok.Value
class ConfigObjectImpl<T> implements ConfigObject<T> {
  T data;
  Instant creationTimestamp;
  Instant lastUpdatedTimestamp;
  String createdByEmail;
  String lastUserUpdateEmail;
  String lastUpdateEmail;

  static <T> Optional<ConfigObject<T>> tryBuild(
      ContextSpecificConfig contextSpecificConfig, Function<Value, Optional<T>> dataBuilder) {
    return tryBuild(
        contextSpecificConfig.getConfig(),
        contextSpecificConfig.getCreationTimestamp(),
        contextSpecificConfig.getUpdateTimestamp(),
        contextSpecificConfig.getCreatedByEmail(),
        contextSpecificConfig.getLastUserUpdateEmail(),
        contextSpecificConfig.getLastUpdateEmail(),
        dataBuilder);
  }

  static <T> Optional<ConfigObject<T>> tryBuild(
      UpsertConfigResponse upsertResponse, Function<Value, Optional<T>> dataBuilder) {
    return tryBuild(
        upsertResponse.getConfig(),
        upsertResponse.getCreationTimestamp(),
        upsertResponse.getUpdateTimestamp(),
        upsertResponse.getCreatedByEmail(),
        upsertResponse.getLastUserUpdateEmail(),
        upsertResponse.getLastUpdateEmail(),
        dataBuilder);
  }

  static <T> Optional<ConfigObject<T>> tryBuild(
      Value config,
      long creationTimestamp,
      long updateTimestamp,
      String createdByEmail,
      String lastUserUpdateEmail,
      String lastUpdateEmail,
      Function<Value, Optional<T>> dataBuilder) {
    return dataBuilder
        .apply(config)
        .map(
            data ->
                new ConfigObjectImpl<>(
                    data,
                    Instant.ofEpochMilli(creationTimestamp),
                    Instant.ofEpochMilli(updateTimestamp),
                    createdByEmail,
                    lastUserUpdateEmail,
                    lastUpdateEmail));
  }
}
