package org.hypertrace.tenantpartitioning.config.service.store;

import java.io.IOException;
import java.util.Optional;
import org.hypertrace.core.documentstore.*;
import org.hypertrace.tenantpartitioning.config.service.v1.TenantPartitionGroupsConfig;

public class TenantPartitionGroupsConfigDocumentStore implements TenantPartitionGroupsConfigStore {
  public static final String TENANT_ISOLATION_CONFIG_COLLECTION = "tenant_isolation_config";
  private static final String ID_FIELD_NAME = "id";

  private final Collection collection;

  public TenantPartitionGroupsConfigDocumentStore(Datastore datastore) {
    this.collection = datastore.getCollection(TENANT_ISOLATION_CONFIG_COLLECTION);
  }

  @Override
  public Optional<TenantPartitionGroupsConfig> getConfig() throws IOException {
    try (CloseableIterator<Document> it = collection.search(new Query())) {
      if (it.hasNext()) {
        String json = it.next().toJson();
        return Optional.of(TenantPartitionGroupsConfigDocument.fromJson(json));
      }
    }
    return Optional.empty();
  }

  @Override
  public TenantPartitionGroupsConfig putConfig(TenantPartitionGroupsConfig config)
      throws IOException {
    Document upsertedDoc =
        this.collection.upsertAndReturn(
            new TenantPartitionGroupsConfigKey(), new TenantPartitionGroupsConfigDocument(config));
    return TenantPartitionGroupsConfigDocument.fromJson(upsertedDoc.toJson());
  }

  @Override
  public void deleteConfig() {
    this.collection.deleteAll();
  }
}
