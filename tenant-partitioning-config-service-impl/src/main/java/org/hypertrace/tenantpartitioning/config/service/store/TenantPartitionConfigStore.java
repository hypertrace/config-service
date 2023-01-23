package org.hypertrace.tenantpartitioning.config.service.store;

import java.io.IOException;
import java.util.List;
import org.hypertrace.tenantpartitioning.config.service.v1.TenantPartitionGroupConfig;

public interface TenantPartitionConfigStore {
  TenantPartitionGroupConfig upsert(TenantPartitionGroupConfig tenantIsolationGroupConfig)
      throws IOException;

  boolean delete(String id);

  List<TenantPartitionGroupConfig> getAllGroupConfigs() throws IOException;
}
