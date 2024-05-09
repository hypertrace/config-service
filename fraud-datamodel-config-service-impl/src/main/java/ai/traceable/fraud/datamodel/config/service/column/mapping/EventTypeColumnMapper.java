package ai.traceable.fraud.datamodel.config.service.column.mapping;

import ai.traceable.fraud.datamodel.config.service.v1.EventType;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeColumnMappings;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.List;

public class EventTypeColumnMapper implements ColumnMapper<EventType> {
  private final ColumnMapperDelegate delegate;

  @Inject
  protected EventTypeColumnMapper(ColumnMapperDelegate delegate) {
    this.delegate = delegate;
  }

  @Override
  public ObjectTypeColumnMappings createColumnMapping(EventType newType) {
    return ObjectTypeColumnMappings.newBuilder()
        .putAllFieldsMeta(newType.getFieldsMetaMap())
        // these are not allowed to change, so if user updates it this will undo that
        // todo: add default mappings here.
        .build();
  }

  @Override
  public EventType forCreate(String tenantId, EventType inputType) throws IOException {
    String typeId = inputType.getId();
    EventType.Builder newTypeBldr = inputType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            tenantId, ObjectKind.OBJECT_KIND_EVENT, typeId, createColumnMapping(inputType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public EventType forUpdate(String tenantId, EventType currType, EventType newType)
      throws IOException {
    // assumes that the type validator is invoked beforehand
    String typeId = newType.getId();
    EventType.Builder newTypeBldr = newType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            tenantId, ObjectKind.OBJECT_KIND_EVENT, typeId, createColumnMapping(newType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public EventType populateFieldMappings(
      EventType objectType, List<ColumnMappingsDocument> mappings) {
    ObjectTypeColumnMappings typeColumnMappings = delegate.buildObjectTypeColumnMappings(mappings);
    var newTypeBldr = objectType.toBuilder();
    newTypeBldr.clearFieldsMeta().clearColumnMappingMeta();
    newTypeBldr.setColumnMappingMeta(typeColumnMappings.getColumnMappingMeta());
    newTypeBldr.putAllFieldsMeta(typeColumnMappings.getFieldsMetaMap());
    return newTypeBldr.build();
  }
}
