package org.hypertrace.tenantisolation.config.service.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.documentstore.*;

public class TenantIsolationConfigDocumentStore implements TenantIsolationConfigStore {
  public static final String TENANT_ISOLATION_CONFIG_COLLECTION = "tenant_isolation_config";

  private final Collection collection;

  public TenantIsolationConfigDocumentStore(Datastore datastore) {
    this.collection = datastore.getCollection(TENANT_ISOLATION_CONFIG_COLLECTION);
  }

  @Override
  public TenantIsolationGroupConfigDTO upsert(
      TenantIsolationGroupConfigDTO tenantIsolationGroupConfigDTO) throws IOException {
    TenantIsolationGroupConfigKey key =
        new TenantIsolationGroupConfigKey(tenantIsolationGroupConfigDTO.getGroupName());
    Document document = new TenantIsolationGroupConfigDocument(tenantIsolationGroupConfigDTO);
    Document upserted = this.collection.upsertAndReturn(key, document);
    return TenantIsolationGroupConfigDocument.fromJson(upserted.toJson());
  }

  @Override
  public boolean delete(String groupName) {
    TenantIsolationGroupConfigKey key = new TenantIsolationGroupConfigKey(groupName);
    return this.collection.delete(key);
  }

  @Override
  public List<TenantIsolationGroupConfigDTO> getAllGroupConfigs() throws IOException {
    List<TenantIsolationGroupConfigDTO> result = new ArrayList<>();
    try (CloseableIterator<Document> it = collection.search(new Query())) {
      while (it.hasNext()) {
        String json = it.next().toJson();
        TenantIsolationGroupConfigDTO configDTO = TenantIsolationGroupConfigDocument.fromJson(json);
        result.add(configDTO);
      }
    }
    return result;
  }
}
