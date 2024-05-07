package ai.traceable.fraud.datamodel.config.service;

import ai.traceable.fraud.datamodel.config.service.store.FraudObjectTypesDocumentStore;
import ai.traceable.fraud.datamodel.config.service.store.FraudObjectTypesStore;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;

public class FraudDataModelConfigServiceModule extends AbstractModule {
  public static final String GENERIC_CONFIG_SERVICE = "generic.config.service";
  public static final String DOC_STORE_CONFIG_KEY = "document.store";
  public static final String DATA_STORE_TYPE = "dataStoreType";
  private final Config config;

  public FraudDataModelConfigServiceModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(FraudDataModelConfigServiceImpl.class);
    bind(FraudObjectTypesStore.class).toInstance(getDocumentStore(config));
  }

  private FraudObjectTypesDocumentStore getDocumentStore(Config config) {
    Config genericConfig = config.getConfig(GENERIC_CONFIG_SERVICE);
    Config docStoreConfig = genericConfig.getConfig(DOC_STORE_CONFIG_KEY);
    String dataStoreType = docStoreConfig.getString(DATA_STORE_TYPE);
    Config dataStoreConfig = docStoreConfig.getConfig(dataStoreType);
    Datastore datastore = DatastoreProvider.getDatastore(dataStoreType, dataStoreConfig);
    return new FraudObjectTypesDocumentStore(datastore);
  }
}
