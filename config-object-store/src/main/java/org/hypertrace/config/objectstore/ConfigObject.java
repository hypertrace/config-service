package org.hypertrace.config.objectstore;

import java.time.Instant;

public interface ConfigObject<T> {
  T getData();

  Instant getCreationTimestamp();

  String getCreatedByEmail();

  Instant getLastUserUpdateTimestamp();

  String getLastUserUpdateEmail();

  Instant getLastUpdatedTimestamp();

  String getLastUpdateEmail();
}
