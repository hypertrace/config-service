package org.hypertrace.tenantpartitioning.config.service;

import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.tenantpartitioning.config.service.store.TenantPartitionGroupsConfigDocumentStore;
import org.hypertrace.tenantpartitioning.config.service.store.TenantPartitionGroupsConfigStore;

public class TenantPartitioningConfigServiceModule extends AbstractModule {

  public static final String GENERIC_CONFIG_SERVICE = "generic.config.service";
  public static final String DOC_STORE_CONFIG_KEY = "document.store";
  public static final String DATA_STORE_TYPE = "dataStoreType";

  private final Config config;

  public TenantPartitioningConfigServiceModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(Config.class).toInstance(this.config);
    bind(BindableService.class).to(TenantPartitioningConfigServiceImpl.class);
    bind(TenantPartitionGroupsConfigStore.class)
        .toInstance(getDocumentStore(config.getConfig(GENERIC_CONFIG_SERVICE)));
  }

  private TenantPartitionGroupsConfigDocumentStore getDocumentStore(Config config) {
    Config docStoreConfig = config.getConfig(DOC_STORE_CONFIG_KEY);
    String dataStoreType = docStoreConfig.getString(DATA_STORE_TYPE);
    Config dataStoreConfig = docStoreConfig.getConfig(dataStoreType);
    Datastore datastore = DatastoreProvider.getDatastore(dataStoreType, dataStoreConfig);
    return new TenantPartitionGroupsConfigDocumentStore(datastore);
  }
}
