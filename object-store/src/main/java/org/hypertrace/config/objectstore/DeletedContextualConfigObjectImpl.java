package org.hypertrace.config.objectstore;

import com.google.protobuf.Value;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;
import org.hypertrace.config.service.v1.ContextSpecificConfig;

@lombok.Value
class DeletedContextualConfigObjectImpl<T> implements DeletedContextualConfigObject<T> {
  String context;
  Optional<T> optionalData;
  Instant creationTimestamp;
  Instant lastUpdatedTimestamp;

  DeletedContextualConfigObjectImpl(
      String context, T data, Instant creationTimestamp, Instant lastUpdatedTimestamp) {
    this.context = context;
    this.optionalData = Optional.of(data);
    this.creationTimestamp = creationTimestamp;
    this.lastUpdatedTimestamp = lastUpdatedTimestamp;
  }

  DeletedContextualConfigObjectImpl(
      String context, Instant creationTimestamp, Instant lastUpdatedTimestamp) {
    this.context = context;
    this.optionalData = Optional.empty();
    this.creationTimestamp = creationTimestamp;
    this.lastUpdatedTimestamp = lastUpdatedTimestamp;
  }

  static <T> DeletedContextualConfigObject<T> tryBuild(
      ContextSpecificConfig contextSpecificConfig, Function<Value, Optional<T>> dataBuilder) {
    return tryBuild(
        contextSpecificConfig.getContext(),
        contextSpecificConfig.getConfig(),
        contextSpecificConfig.getCreationTimestamp(),
        contextSpecificConfig.getUpdateTimestamp(),
        dataBuilder);
  }

  private static <T> DeletedContextualConfigObject<T> tryBuild(
      String context,
      Value config,
      long creationTimestamp,
      long updateTimestamp,
      Function<Value, Optional<T>> dataBuilder) {
    return dataBuilder
        .apply(config)
        .map(
            data ->
                new DeletedContextualConfigObjectImpl<>(
                    context,
                    data,
                    Instant.ofEpochMilli(creationTimestamp),
                    Instant.ofEpochMilli(updateTimestamp)))
        .orElse(
            new DeletedContextualConfigObjectImpl<>(
                context,
                Instant.ofEpochMilli(creationTimestamp),
                Instant.ofEpochMilli(updateTimestamp)));
  }
}
