package org.hypertrace.tenantpartitioning.config.service.store;

import java.util.UUID;
import lombok.Value;
import org.hypertrace.core.documentstore.Key;

@Value
public class TenantPartitionGroupConfigKey implements Key {
  String groupName;

  @Override
  public String toString() {
    return UUID.nameUUIDFromBytes((groupName).getBytes()).toString();
  }
}
