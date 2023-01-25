package org.hypertrace.config.objectstore;

import java.time.Instant;
import java.util.Optional;

public interface DeletedConfigObject<T> {
  Optional<T> getDeletedData();

  Instant getCreationTimestamp();

  Instant getLastUpdatedTimestamp();
}
