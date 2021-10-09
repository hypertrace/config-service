package org.hypertrace.config.service.store;

import java.util.Optional;
import lombok.Value;
import org.hypertrace.config.service.v1.ContextSpecificConfig;

@Value
public class InternalContextSpecificConfig {
  ContextSpecificConfig contextSpecificConfig;
  Optional<com.google.protobuf.Value> prevConfig;
}
