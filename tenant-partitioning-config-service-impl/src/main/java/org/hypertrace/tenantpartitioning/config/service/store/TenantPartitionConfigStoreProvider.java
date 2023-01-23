package org.hypertrace.tenantpartitioning.config.service.store;

import com.typesafe.config.Config;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;

public class TenantPartitionConfigStoreProvider {
  public static final String DOC_STORE_CONFIG_KEY = "document.store";
  public static final String DATA_STORE_TYPE = "dataStoreType";

  private TenantPartitionConfigStoreProvider() {}

  public static TenantPartitionConfigDocumentStore getDocumentStore(Config config) {
    Config docStoreConfig = config.getConfig(DOC_STORE_CONFIG_KEY);
    String dataStoreType = docStoreConfig.getString(DATA_STORE_TYPE);
    Config dataStoreConfig = docStoreConfig.getConfig(dataStoreType);
    Datastore datastore = DatastoreProvider.getDatastore(dataStoreType, dataStoreConfig);
    return new TenantPartitionConfigDocumentStore(datastore);
  }
}
