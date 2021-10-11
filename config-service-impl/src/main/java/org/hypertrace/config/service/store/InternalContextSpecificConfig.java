package org.hypertrace.config.service.store;

import java.util.Optional;
import lombok.Value;
import org.hypertrace.config.service.v1.ContextSpecificConfig;

@Value
public class InternalContextSpecificConfig {
  private final ContextSpecificConfig contextSpecificConfig;
  private final Optional<com.google.protobuf.Value> prevConfigOptional;

  public InternalContextSpecificConfig(ContextSpecificConfig contextSpecificConfig) {
    this.contextSpecificConfig = contextSpecificConfig;
    this.prevConfigOptional = Optional.empty();
  }

  public InternalContextSpecificConfig(
      ContextSpecificConfig contextSpecificConfig, com.google.protobuf.Value prevConfig) {
    this.contextSpecificConfig = contextSpecificConfig;
    this.prevConfigOptional = Optional.of(prevConfig);
  }
}
