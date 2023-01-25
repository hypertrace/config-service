package org.hypertrace.config.objectstore;

import java.time.Instant;
import java.util.Optional;

public interface ConfigObject<T> {
  Optional<T> getData();

  Instant getCreationTimestamp();

  Instant getLastUpdatedTimestamp();
}
