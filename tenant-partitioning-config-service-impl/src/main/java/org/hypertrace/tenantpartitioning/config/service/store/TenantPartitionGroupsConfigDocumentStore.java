package org.hypertrace.tenantpartitioning.config.service.store;

import java.io.IOException;
import java.util.*;

import com.google.common.collect.ImmutableList;
import org.hypertrace.core.documentstore.*;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.tenantpartitioning.config.service.v1.TenantPartitionGroupsConfig;

public class TenantPartitionGroupsConfigDocumentStore implements TenantPartitionGroupsConfigStore {
  public static final String TENANT_PARTITION_GROUPS_CONFIG = "tenant_partition_groups_config";

  private final Collection collection;

  public TenantPartitionGroupsConfigDocumentStore(Datastore datastore) {
    this.collection = datastore.getCollection(TENANT_PARTITION_GROUPS_CONFIG);
  }

  @Override
  public Optional<TenantPartitionGroupsConfig> getConfig() throws IOException {
    try (CloseableIterator<Document> it = collection.search(new Query())) {
      List<Document> list = ImmutableList.copyOf(it);
      if (list.size() == 1) {
        return Optional.of(TenantPartitionGroupsConfigDocument.fromJson(list.get(0).toJson()));
      } else if (list.size() > 1) {
        throw new IllegalStateException("More than 1 TenantPartitionGroupsConfig returned");
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
}
