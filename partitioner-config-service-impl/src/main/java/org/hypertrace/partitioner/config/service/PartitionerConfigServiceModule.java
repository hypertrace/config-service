package org.hypertrace.partitioner.config.service;

import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.partitioner.config.service.store.PartitionerProfilesDocumentStore;
import org.hypertrace.partitioner.config.service.store.PartitionerProfilesStore;

public class PartitionerConfigServiceModule extends AbstractModule {

  public static final String GENERIC_CONFIG_SERVICE = "generic.config.service";
  public static final String DOC_STORE_CONFIG_KEY = "document.store";
  public static final String DATA_STORE_TYPE = "dataStoreType";
  public static final String PARTITIONER_CONFIG_SERVICE = "partitioner.config.service";

  private final Config config;
  private Datastore datastore;

  @Deprecated
  public PartitionerConfigServiceModule(Config config) {
    this.config = config;
  }

  public PartitionerConfigServiceModule(Config config, Datastore datastore) {
    this.config = config;
    this.datastore = datastore;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(PartitionerConfigServiceImpl.class);
    bind(PartitionerProfilesStore.class).toInstance(getDocumentStore(config, datastore));
  }

  private PartitionerProfilesDocumentStore getDocumentStore(Config config, Datastore datastore) {
    Config partitionerConfig = config.getConfig(PARTITIONER_CONFIG_SERVICE);
    return new PartitionerProfilesDocumentStore(datastore, partitionerConfig);
  }
}
