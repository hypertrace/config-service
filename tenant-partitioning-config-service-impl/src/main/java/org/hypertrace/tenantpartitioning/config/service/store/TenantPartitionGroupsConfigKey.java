package org.hypertrace.tenantpartitioning.config.service.store;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import lombok.Value;
import org.hypertrace.core.documentstore.Key;

@Value
public class TenantPartitionGroupsConfigKey implements Key {

  // some fixed random string
  private static final String uuidStr =
      UUID.nameUUIDFromBytes(
              "global+config+66106906-ba01-4d20-ad85-3bbc8cb01fe9".getBytes(StandardCharsets.UTF_8))
          .toString();

  @Override
  public String toString() {
    return uuidStr;
  }
}
