package org.hypertrace.config.objectstore;

import com.google.protobuf.Value;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;
import lombok.Builder;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse.UpsertedConfig;
import org.hypertrace.config.service.v1.UpsertConfigResponse;

@lombok.Value
@Builder(toBuilder = true)
class ContextualConfigObjectImpl<T> implements ContextualConfigObject<T> {
  String context;
  T data;
  Instant creationTimestamp;
  Instant lastUpdatedTimestamp;

  static <T> Optional<ContextualConfigObject<T>> tryBuild(
      ContextSpecificConfig contextSpecificConfig, Function<Value, Optional<T>> dataBuilder) {
    return tryBuild(
        contextSpecificConfig.getContext(),
        contextSpecificConfig.getConfig(),
        contextSpecificConfig.getCreationTimestamp(),
        contextSpecificConfig.getUpdateTimestamp(),
        dataBuilder);
  }

  static <T> Optional<ContextualConfigObject<T>> tryBuild(
      UpsertedConfig upsertedConfig, Function<Value, Optional<T>> dataBuilder) {
    return tryBuild(
        upsertedConfig.getContext(),
        upsertedConfig.getConfig(),
        upsertedConfig.getCreationTimestamp(),
        upsertedConfig.getUpdateTimestamp(),
        dataBuilder);
  }

  static <T> Optional<ContextualConfigObject<T>> tryBuild(
      UpsertConfigResponse upsertResponse,
      Function<Value, Optional<T>> dataBuilder,
      Function<T, String> contextBuilder) {
    return dataBuilder
        .apply(upsertResponse.getConfig())
        .map(
            data ->
                new ContextualConfigObjectImpl<>(
                    contextBuilder.apply(data),
                    data,
                    Instant.ofEpochMilli(upsertResponse.getCreationTimestamp()),
                    Instant.ofEpochMilli(upsertResponse.getUpdateTimestamp())));
  }

  private static <T> Optional<ContextualConfigObject<T>> tryBuild(
      String context,
      Value config,
      long creationTimestamp,
      long updateTimestamp,
      Function<Value, Optional<T>> dataBuilder) {
    return dataBuilder
        .apply(config)
        .map(
            data ->
                new ContextualConfigObjectImpl<>(
                    context,
                    data,
                    Instant.ofEpochMilli(creationTimestamp),
                    Instant.ofEpochMilli(updateTimestamp)));
  }
}
