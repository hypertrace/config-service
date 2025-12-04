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
  String createdByEmail;
  String lastUserUpdateEmail;
  String lastUpdateEmail;

  static <T> Optional<ContextualConfigObject<T>> tryBuild(
      ContextSpecificConfig contextSpecificConfig, Function<Value, Optional<T>> dataBuilder) {
    return tryBuild(
        contextSpecificConfig.getContext(),
        contextSpecificConfig.getConfig(),
        contextSpecificConfig.getCreationTimestamp(),
        contextSpecificConfig.getUpdateTimestamp(),
        contextSpecificConfig.getCreatedByEmail(),
        contextSpecificConfig.getLastUserUpdateEmail(),
        contextSpecificConfig.getLastUpdateEmail(),
        dataBuilder);
  }

  static <T> Optional<ContextualConfigObject<T>> tryBuild(
      UpsertedConfig upsertedConfig, Function<Value, Optional<T>> dataBuilder) {
    return tryBuild(
        upsertedConfig.getContext(),
        upsertedConfig.getConfig(),
        upsertedConfig.getCreationTimestamp(),
        upsertedConfig.getUpdateTimestamp(),
        upsertedConfig.getCreatedByEmail(),
        upsertedConfig.getLastUserUpdateEmail(),
        upsertedConfig.getLastUpdateEmail(),
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
                    Instant.ofEpochMilli(upsertResponse.getUpdateTimestamp()),
                    upsertResponse.getCreatedByEmail(),
                    upsertResponse.getLastUserUpdateEmail(),
                    upsertResponse.getLastUpdateEmail()));
  }

  private static <T> Optional<ContextualConfigObject<T>> tryBuild(
      String context,
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
                new ContextualConfigObjectImpl<>(
                    context,
                    data,
                    Instant.ofEpochMilli(creationTimestamp),
                    Instant.ofEpochMilli(updateTimestamp),
                    createdByEmail,
                    lastUserUpdateEmail,
                    lastUpdateEmail));
  }
}
