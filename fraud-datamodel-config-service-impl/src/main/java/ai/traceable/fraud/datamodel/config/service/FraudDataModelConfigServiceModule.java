package ai.traceable.fraud.datamodel.config.service;

import ai.traceable.fraud.datamodel.config.service.column.mapping.BaselineTypeColumnMapper;
import ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMapper;
import ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMapperDelegate;
import ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMapperDelegateImpl;
import ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMappingsDocumentStore;
import ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMappingsStore;
import ai.traceable.fraud.datamodel.config.service.column.mapping.EntityTypeColumnMapper;
import ai.traceable.fraud.datamodel.config.service.column.mapping.EventTypeColumnMapper;
import ai.traceable.fraud.datamodel.config.service.column.mapping.MetricTypeColumnMapper;
import ai.traceable.fraud.datamodel.config.service.column.mapping.RelationshipTypeColumnMapper;
import ai.traceable.fraud.datamodel.config.service.store.FraudObjectTypesDocumentStore;
import ai.traceable.fraud.datamodel.config.service.store.FraudObjectTypesStore;
import ai.traceable.fraud.datamodel.config.service.v1.BaselineType;
import ai.traceable.fraud.datamodel.config.service.v1.EntityType;
import ai.traceable.fraud.datamodel.config.service.v1.EventType;
import ai.traceable.fraud.datamodel.config.service.v1.MetricType;
import ai.traceable.fraud.datamodel.config.service.v1.RelationshipType;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;

public class FraudDataModelConfigServiceModule extends AbstractModule {
  public static final String GENERIC_CONFIG_SERVICE = "generic.config.service";
  public static final String DOC_STORE_CONFIG_KEY = "document.store";
  public static final String DATA_STORE_TYPE = "dataStoreType";
  private final Config config;
  private final ConfigChangeEventGenerator changeEventGenerator;

  public FraudDataModelConfigServiceModule(
      Config config, ConfigChangeEventGenerator changeEventGenerator) {
    this.config = config;
    this.changeEventGenerator = changeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(BindableService.class).to(FraudDataModelConfigServiceImpl.class);
    bind(FraudObjectTypesStore.class)
        .toInstance(getFraudObjectTypesDocumentStore(config, changeEventGenerator));
    bind(ColumnMappingsStore.class).toInstance(getColumnMappingsDocumentStore(config));
    bind(ColumnMapperDelegate.class).to(ColumnMapperDelegateImpl.class);
    bind(new TypeLiteral<ColumnMapper<EntityType>>() {}).to(EntityTypeColumnMapper.class);
    bind(new TypeLiteral<ColumnMapper<RelationshipType>>() {})
        .to(RelationshipTypeColumnMapper.class);
    bind(new TypeLiteral<ColumnMapper<EventType>>() {}).to(EventTypeColumnMapper.class);
    bind(new TypeLiteral<ColumnMapper<MetricType>>() {}).to(MetricTypeColumnMapper.class);
    bind(new TypeLiteral<ColumnMapper<BaselineType>>() {}).to(BaselineTypeColumnMapper.class);
  }

  private FraudObjectTypesDocumentStore getFraudObjectTypesDocumentStore(
      Config config, ConfigChangeEventGenerator changeEventGenerator) {
    Config genericConfig = config.getConfig(GENERIC_CONFIG_SERVICE);
    Config docStoreConfig = genericConfig.getConfig(DOC_STORE_CONFIG_KEY);
    String dataStoreType = docStoreConfig.getString(DATA_STORE_TYPE);
    Config dataStoreConfig = docStoreConfig.getConfig(dataStoreType);
    Datastore datastore = DatastoreProvider.getDatastore(dataStoreType, dataStoreConfig);
    return new FraudObjectTypesDocumentStore(datastore, changeEventGenerator);
  }

  private ColumnMappingsDocumentStore getColumnMappingsDocumentStore(Config config) {
    Config genericConfig = config.getConfig(GENERIC_CONFIG_SERVICE);
    Config docStoreConfig = genericConfig.getConfig(DOC_STORE_CONFIG_KEY);
    String dataStoreType = docStoreConfig.getString(DATA_STORE_TYPE);
    Config dataStoreConfig = docStoreConfig.getConfig(dataStoreType);
    Datastore datastore = DatastoreProvider.getDatastore(dataStoreType, dataStoreConfig);
    return new ColumnMappingsDocumentStore(datastore);
  }
}
