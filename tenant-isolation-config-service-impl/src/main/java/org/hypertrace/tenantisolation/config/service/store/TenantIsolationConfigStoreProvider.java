package org.hypertrace.tenantisolation.config.service.store;

import com.typesafe.config.Config;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;

public class TenantIsolationConfigStoreProvider {
  public static final String DOC_STORE_CONFIG_KEY = "document.store";
  public static final String DATA_STORE_TYPE = "dataStoreType";

  private TenantIsolationConfigStoreProvider() {}

  public static TenantIsolationConfigDocumentStore getDocumentStore(Config config) {
    Config docStoreConfig = config.getConfig(DOC_STORE_CONFIG_KEY);
    String dataStoreType = docStoreConfig.getString(DATA_STORE_TYPE);
    Config dataStoreConfig = docStoreConfig.getConfig(dataStoreType);
    Datastore datastore = DatastoreProvider.getDatastore(dataStoreType, dataStoreConfig);
    return new TenantIsolationConfigDocumentStore(datastore);
  }
}
