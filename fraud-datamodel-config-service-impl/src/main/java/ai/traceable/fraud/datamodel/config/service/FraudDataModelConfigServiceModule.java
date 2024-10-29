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
import io.grpc.BindableService;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.documentstore.Datastore;

public class FraudDataModelConfigServiceModule extends AbstractModule {
  private final ConfigChangeEventGenerator changeEventGenerator;
  private final Datastore datastore;

  public FraudDataModelConfigServiceModule(
      ConfigChangeEventGenerator changeEventGenerator, Datastore datastore) {
    this.changeEventGenerator = changeEventGenerator;
    this.datastore = datastore;
  }

  @Override
  protected void configure() {
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(BindableService.class).to(FraudDataModelConfigServiceImpl.class);
    bind(FraudObjectTypesStore.class)
        .toInstance(new FraudObjectTypesDocumentStore(datastore, changeEventGenerator));
    bind(ColumnMappingsStore.class).toInstance(new ColumnMappingsDocumentStore(datastore));
    bind(ColumnMapperDelegate.class).to(ColumnMapperDelegateImpl.class);
    bind(new TypeLiteral<ColumnMapper<EntityType>>() {}).to(EntityTypeColumnMapper.class);
    bind(new TypeLiteral<ColumnMapper<RelationshipType>>() {})
        .to(RelationshipTypeColumnMapper.class);
    bind(new TypeLiteral<ColumnMapper<EventType>>() {}).to(EventTypeColumnMapper.class);
    bind(new TypeLiteral<ColumnMapper<MetricType>>() {}).to(MetricTypeColumnMapper.class);
    bind(new TypeLiteral<ColumnMapper<BaselineType>>() {}).to(BaselineTypeColumnMapper.class);
  }
}
