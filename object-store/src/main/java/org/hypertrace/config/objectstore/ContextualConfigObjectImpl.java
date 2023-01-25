package org.hypertrace.config.objectstore;

import com.google.protobuf.Value;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse.UpsertedConfig;
import org.hypertrace.config.service.v1.UpsertConfigResponse;

@Slf4j
@lombok.Value
class ContextualConfigObjectImpl<T> implements ContextualConfigObject<T> {
  String context;
  Optional<T> data;
  Instant creationTimestamp;
  Instant lastUpdatedTimestamp;

  ContextualConfigObjectImpl(
      String context, T data, Instant creationTimestamp, Instant lastUpdatedTimestamp) {
    this.context = context;
    this.data = Optional.of(data);
    this.creationTimestamp = creationTimestamp;
    this.lastUpdatedTimestamp = lastUpdatedTimestamp;
  }

  ContextualConfigObjectImpl(
      String context, Instant creationTimestamp, Instant lastUpdatedTimestamp) {
    this.context = context;
    this.data = Optional.empty();
    this.creationTimestamp = creationTimestamp;
    this.lastUpdatedTimestamp = lastUpdatedTimestamp;
  }

  static <T> ContextualConfigObject<T> tryBuild(
      ContextSpecificConfig contextSpecificConfig, Function<Value, Optional<T>> dataBuilder) {
    return tryBuild(
        contextSpecificConfig.getContext(),
        contextSpecificConfig.getConfig(),
        contextSpecificConfig.getCreationTimestamp(),
        contextSpecificConfig.getUpdateTimestamp(),
        dataBuilder);
  }

  static <T> ContextualConfigObject<T> tryBuild(
      UpsertedConfig upsertedConfig, Function<Value, Optional<T>> dataBuilder) {
    return tryBuild(
        upsertedConfig.getContext(),
        upsertedConfig.getConfig(),
        upsertedConfig.getCreationTimestamp(),
        upsertedConfig.getUpdateTimestamp(),
        dataBuilder);
  }

  static <T> ContextualConfigObject<T> tryBuild(
      UpsertConfigResponse upsertResponse,
      Function<Value, Optional<T>> dataBuilder,
      Function<T, String> contextBuilder) {
    Optional<ContextualConfigObjectImpl<T>> optionalContextualConfigObject =
        dataBuilder
            .apply(upsertResponse.getConfig())
            .map(
                data ->
                    new ContextualConfigObjectImpl<>(
                        contextBuilder.apply(data),
                        data,
                        Instant.ofEpochMilli(upsertResponse.getCreationTimestamp()),
                        Instant.ofEpochMilli(upsertResponse.getUpdateTimestamp())));
    if (optionalContextualConfigObject.isEmpty()) {
      log.warn("Could not parse value:{} into proto message", upsertResponse.getConfig());
      return new ContextualConfigObjectImpl<>(
          "",
          Instant.ofEpochMilli(upsertResponse.getCreationTimestamp()),
          Instant.ofEpochMilli(upsertResponse.getUpdateTimestamp()));
    }
    return optionalContextualConfigObject.get();
  }

  private static <T> ContextualConfigObject<T> tryBuild(
      String context,
      Value config,
      long creationTimestamp,
      long updateTimestamp,
      Function<Value, Optional<T>> dataBuilder) {
    Optional<ContextualConfigObjectImpl<T>> optionalContextualConfigObject =
        dataBuilder
            .apply(config)
            .map(
                data ->
                    new ContextualConfigObjectImpl<>(
                        context,
                        data,
                        Instant.ofEpochMilli(creationTimestamp),
                        Instant.ofEpochMilli(updateTimestamp)));
    if (optionalContextualConfigObject.isEmpty()) {
      log.warn("Could not parse value:{} into proto message", config);
      return optionalContextualConfigObject.orElse(
          new ContextualConfigObjectImpl<>(
              context,
              Instant.ofEpochMilli(creationTimestamp),
              Instant.ofEpochMilli(updateTimestamp)));
    }
    return optionalContextualConfigObject.get();
  }
}
