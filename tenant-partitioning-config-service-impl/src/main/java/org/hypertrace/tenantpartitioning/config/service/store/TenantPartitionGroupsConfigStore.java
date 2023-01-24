package org.hypertrace.tenantpartitioning.config.service.store;

import java.io.IOException;
import java.util.Optional;
import org.hypertrace.tenantpartitioning.config.service.v1.TenantPartitionGroupsConfig;

public interface TenantPartitionGroupsConfigStore {

  Optional<TenantPartitionGroupsConfig> getConfig() throws IOException;

  TenantPartitionGroupsConfig putConfig(TenantPartitionGroupsConfig config) throws IOException;

  void deleteConfig();

  //  TenantPartitionGroupConfig upsert(TenantPartitionGroupConfig tenantIsolationGroupConfig)
  //      throws IOException;
  //
  //  boolean delete(String id);
  //
  //  List<TenantPartitionGroupConfig> getAllGroupConfigs() throws IOException;
}
