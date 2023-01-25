package org.hypertrace.config.objectstore;

import com.google.protobuf.Value;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.UpsertConfigResponse;

@lombok.Value
class ConfigObjectImpl<T> implements ConfigObject<T> {
  Optional<T> data;
  Instant creationTimestamp;
  Instant lastUpdatedTimestamp;

  public ConfigObjectImpl(T data, Instant creationTimestamp, Instant lastUpdatedTimestamp) {
    this.data = Optional.of(data);
    this.creationTimestamp = creationTimestamp;
    this.lastUpdatedTimestamp = lastUpdatedTimestamp;
  }

  public ConfigObjectImpl(Instant creationTimestamp, Instant lastUpdatedTimestamp) {
    this.data = Optional.empty();
    this.creationTimestamp = creationTimestamp;
    this.lastUpdatedTimestamp = lastUpdatedTimestamp;
  }

  static <T> ConfigObject<T> tryBuild(
      ContextSpecificConfig contextSpecificConfig, Function<Value, Optional<T>> dataBuilder) {
    return tryBuild(
        contextSpecificConfig.getConfig(),
        contextSpecificConfig.getCreationTimestamp(),
        contextSpecificConfig.getUpdateTimestamp(),
        dataBuilder);
  }

  static <T> ConfigObject<T> tryBuild(
      UpsertConfigResponse upsertResponse, Function<Value, Optional<T>> dataBuilder) {
    return tryBuild(
        upsertResponse.getConfig(),
        upsertResponse.getCreationTimestamp(),
        upsertResponse.getUpdateTimestamp(),
        dataBuilder);
  }

  static <T> ConfigObject<T> tryBuild(
      Value config,
      long creationTimestamp,
      long updateTimestamp,
      Function<Value, Optional<T>> dataBuilder) {
    return dataBuilder
        .apply(config)
        .map(
            data ->
                new ConfigObjectImpl<>(
                    data,
                    Instant.ofEpochMilli(creationTimestamp),
                    Instant.ofEpochMilli(updateTimestamp)))
        .orElse(
            new ConfigObjectImpl<>(
                Instant.ofEpochMilli(creationTimestamp), Instant.ofEpochMilli(updateTimestamp)));
  }
}
