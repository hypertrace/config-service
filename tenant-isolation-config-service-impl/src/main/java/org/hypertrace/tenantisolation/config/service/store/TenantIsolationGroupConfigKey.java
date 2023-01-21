package org.hypertrace.tenantisolation.config.service.store;

import java.util.UUID;
import lombok.Value;
import org.hypertrace.core.documentstore.Key;

@Value
public class TenantIsolationGroupConfigKey implements Key {
  String groupName;

  @Override
  public String toString() {
    return UUID.nameUUIDFromBytes((groupName).getBytes()).toString();
  }
}
