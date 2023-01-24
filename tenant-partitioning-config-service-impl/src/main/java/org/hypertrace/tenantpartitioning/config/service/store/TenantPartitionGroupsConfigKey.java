package org.hypertrace.tenantpartitioning.config.service.store;

import lombok.Value;
import org.hypertrace.core.documentstore.Key;

@Value
public class TenantPartitionGroupsConfigKey implements Key {

  // some fixed
  // string
  private static final String GLOBAL_CONFIG = "global_config";

  @Override
  public String toString() {
    return GLOBAL_CONFIG;
  }
}
