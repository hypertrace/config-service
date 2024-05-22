package ai.traceable.fraud.datamodel.config.service.column.mapping;

import static ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMapperUtils.entityFieldMetaToInternalFieldMetadataMap;
import static ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMapperUtils.toEntityFieldMetadataMap;

import ai.traceable.fraud.datamodel.config.service.v1.EntityType;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeColumnMappings;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EntityTypeColumnMapper implements ColumnMapper<EntityType> {

  private final ColumnMapperDelegate delegate;

  @Inject
  protected EntityTypeColumnMapper(ColumnMapperDelegate delegate) {
    this.delegate = delegate;
  }

  @Override
  public ObjectTypeColumnMappings createColumnMapping(EntityType newType) {
    return ObjectTypeColumnMappings.newBuilder()
        .putAllFieldsMeta(entityFieldMetaToInternalFieldMetadataMap(newType.getFieldsMetaMap()))
        // these are not allowed to change, so if user updates it this will undo that
        // todo: add default mappings here.
        .build();
  }

  @Override
  public EntityType forCreate(RequestContext requestContext, EntityType inputType)
      throws IOException {
    String typeId = inputType.getId();
    EntityType.Builder newTypeBldr = inputType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            requestContext, ObjectKind.OBJECT_KIND_ENTITY, typeId, createColumnMapping(inputType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public EntityType forUpdate(
      RequestContext requestContext, EntityType currType, EntityType newType) throws IOException {
    // assumes that the type validator is invoked beforehand
    String typeId = newType.getId();
    EntityType.Builder newTypeBldr = newType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            requestContext, ObjectKind.OBJECT_KIND_ENTITY, typeId, createColumnMapping(newType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public EntityType populateFieldMappings(
      EntityType objectType, List<ColumnMappingsDocument> mappings) {
    ObjectTypeColumnMappings typeColumnMappings = delegate.buildObjectTypeColumnMappings(mappings);
    var newTypeBldr = objectType.toBuilder();
    newTypeBldr.clearFieldsMeta().clearColumnMappingMeta();
    newTypeBldr.setColumnMappingMeta(typeColumnMappings.getColumnMappingMeta());
    var internalFieldsMetaMap = typeColumnMappings.getFieldsMetaMap();
    newTypeBldr.putAllFieldsMeta(toEntityFieldMetadataMap(internalFieldsMetaMap));
    return newTypeBldr.build();
  }
}
