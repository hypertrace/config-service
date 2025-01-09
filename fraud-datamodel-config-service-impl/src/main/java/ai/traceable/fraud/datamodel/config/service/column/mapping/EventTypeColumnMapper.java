package ai.traceable.fraud.datamodel.config.service.column.mapping;

import static ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMapperUtils.fieldMetaToInternalFieldMetadataMap;
import static ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMapperUtils.toFieldMetadataMap;

import ai.traceable.fraud.datamodel.config.service.v1.EventType;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectTypeColumnMappings;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EventTypeColumnMapper implements ColumnMapper<EventType> {
  private final ColumnMapperDelegate delegate;

  @Inject
  protected EventTypeColumnMapper(ColumnMapperDelegate delegate) {
    this.delegate = delegate;
  }

  @Override
  public ObjectTypeColumnMappings createColumnMapping(EventType newType) {
    return ObjectTypeColumnMappings.newBuilder()
        .putAllFieldsMeta(fieldMetaToInternalFieldMetadataMap(newType.getFieldsMetaMap()))
        .build();
  }

  @Override
  public EventType forCreate(RequestContext requestContext, EventType inputType)
      throws IOException {
    String typeId = inputType.getId();
    EventType.Builder newTypeBldr = inputType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            requestContext, ObjectKind.OBJECT_KIND_EVENT, typeId, createColumnMapping(inputType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public EventType forUpdate(RequestContext requestContext, EventType currType, EventType newType)
      throws IOException {
    // assumes that the type validator is invoked beforehand
    String typeId = newType.getId();
    EventType.Builder newTypeBldr = newType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            requestContext, ObjectKind.OBJECT_KIND_EVENT, typeId, createColumnMapping(newType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public EventType populateFieldMappings(
      EventType objectType, List<ColumnMappingsDocument> mappings) {
    ObjectTypeColumnMappings typeColumnMappings =
        delegate.buildObjectTypeColumnMappings(objectType.getFieldsMetaMap().keySet(), mappings);
    var newTypeBldr = objectType.toBuilder();
    newTypeBldr.clearFieldsMeta().clearColumnMappingMeta();
    newTypeBldr.setColumnMappingMeta(typeColumnMappings.getColumnMappingMeta());
    newTypeBldr.putAllFieldsMeta(toFieldMetadataMap(typeColumnMappings.getFieldsMetaMap()));
    return newTypeBldr.build();
  }
}
