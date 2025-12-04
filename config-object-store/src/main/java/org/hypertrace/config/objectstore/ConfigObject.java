package org.hypertrace.config.objectstore;

import java.time.Instant;

public interface ConfigObject<T> {
  T getData();

  Instant getCreationTimestamp();

  Instant getLastUpdatedTimestamp();

  String getCreatedByEmail();

  String getLastUserUpdateEmail();

  String getLastUpdateEmail();
}
