package org.hypertrace.partitioner.config.service;

import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.partitioner.config.service.store.PartitionerProfilesDocumentStore;
import org.hypertrace.partitioner.config.service.store.PartitionerProfilesStore;

public class PartitionerConfigServiceModule extends AbstractModule {

  public static final String GENERIC_CONFIG_SERVICE = "generic.config.service";
  public static final String DOC_STORE_CONFIG_KEY = "document.store";
  public static final String DATA_STORE_TYPE = "dataStoreType";
  public static final String PARTITIONER_CONFIG_SERVICE = "partitioner.config.service";

  private final Config config;

  public PartitionerConfigServiceModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(PartitionerConfigServiceImpl.class);
    bind(PartitionerProfilesStore.class).toInstance(getDocumentStore(config));
  }

  private PartitionerProfilesDocumentStore getDocumentStore(Config config) {
    Config genericConfig = config.getConfig(GENERIC_CONFIG_SERVICE);
    Config docStoreConfig = genericConfig.getConfig(DOC_STORE_CONFIG_KEY);
    String dataStoreType = docStoreConfig.getString(DATA_STORE_TYPE);
    Config dataStoreConfig = docStoreConfig.getConfig(dataStoreType);
    Config partitionerConfig = config.getConfig(PARTITIONER_CONFIG_SERVICE);
    Datastore datastore = DatastoreProvider.getDatastore(dataStoreType, dataStoreConfig);
    return new PartitionerProfilesDocumentStore(datastore, partitionerConfig);
  }
}
