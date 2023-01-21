package org.hypertrace.tenantisolation.config.service.store;

import java.io.IOException;
import java.util.List;

public interface TenantIsolationConfigStore {
  TenantIsolationGroupConfigDTO upsert(TenantIsolationGroupConfigDTO tenantIsolationGroupConfigDTO)
      throws IOException;

  boolean delete(String groupName);

  List<TenantIsolationGroupConfigDTO> getAllGroupConfigs() throws IOException;
}
