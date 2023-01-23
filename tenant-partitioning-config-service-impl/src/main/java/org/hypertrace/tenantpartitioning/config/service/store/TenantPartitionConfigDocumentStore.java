package org.hypertrace.tenantpartitioning.config.service.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.documentstore.*;
import org.hypertrace.tenantpartitioning.config.service.v1.TenantPartitionGroupConfig;

public class TenantPartitionConfigDocumentStore implements TenantPartitionConfigStore {
  public static final String TENANT_ISOLATION_CONFIG_COLLECTION = "tenant_isolation_config";
  private static final String ID_FIELD_NAME = "id";

  private final Collection collection;

  public TenantPartitionConfigDocumentStore(Datastore datastore) {
    this.collection = datastore.getCollection(TENANT_ISOLATION_CONFIG_COLLECTION);
  }

  @Override
  public TenantPartitionGroupConfig upsert(TenantPartitionGroupConfig tenantIsolationGroupConfig)
      throws IOException {
    TenantPartitionGroupConfigKey key =
        new TenantPartitionGroupConfigKey(tenantIsolationGroupConfig.getGroupName());
    Document document = new TenantPartitionGroupConfigDocument(tenantIsolationGroupConfig);
    Document upserted = this.collection.upsertAndReturn(key, document);
    return TenantPartitionGroupConfigDocument.fromJson(upserted.toJson());
  }

  @Override
  public boolean delete(String id) {
    return this.collection.delete(new Filter(Filter.Op.EQ, ID_FIELD_NAME, id));
  }

  @Override
  public List<TenantPartitionGroupConfig> getAllGroupConfigs() throws IOException {
    List<TenantPartitionGroupConfig> result = new ArrayList<>();
    try (CloseableIterator<Document> it = collection.search(new Query())) {
      while (it.hasNext()) {
        String json = it.next().toJson();
        TenantPartitionGroupConfig config = TenantPartitionGroupConfigDocument.fromJson(json);
        result.add(config);
      }
    }
    return result;
  }
}
