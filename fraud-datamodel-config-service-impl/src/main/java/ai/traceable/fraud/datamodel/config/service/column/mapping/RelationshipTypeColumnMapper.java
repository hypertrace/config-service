package ai.traceable.fraud.datamodel.config.service.column.mapping;

import static ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMapperUtils.entityFieldMetaToInternalFieldMetadataMap;
import static ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMapperUtils.toEntityFieldMetadataMap;

import ai.traceable.fraud.datamodel.config.service.v1.RelationshipType;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeColumnMappings;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RelationshipTypeColumnMapper implements ColumnMapper<RelationshipType> {

  private final ColumnMapperDelegate delegate;

  @Inject
  protected RelationshipTypeColumnMapper(ColumnMapperDelegate delegate) {
    this.delegate = delegate;
  }

  @Override
  public ObjectTypeColumnMappings createColumnMapping(RelationshipType newType) {
    return ObjectTypeColumnMappings.newBuilder()
        .putAllFieldsMeta(entityFieldMetaToInternalFieldMetadataMap(newType.getFieldsMetaMap()))
        // these are not allowed to change, so if user updates it this will undo that
        // todo: add default mappings here.
        .build();
  }

  @Override
  public RelationshipType forCreate(RequestContext requestContext, RelationshipType inputType)
      throws IOException {
    String typeId = inputType.getId();
    RelationshipType.Builder newTypeBldr = inputType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            requestContext,
            ObjectKind.OBJECT_KIND_RELATIONSHIP,
            typeId,
            createColumnMapping(inputType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public RelationshipType forUpdate(
      RequestContext requestContext, RelationshipType currType, RelationshipType newType)
      throws IOException {
    // assumes that the type validator is invoked beforehand
    String typeId = newType.getId();
    RelationshipType.Builder newTypeBldr = newType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            requestContext,
            ObjectKind.OBJECT_KIND_RELATIONSHIP,
            typeId,
            createColumnMapping(newType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public RelationshipType populateFieldMappings(
      RelationshipType objectType, List<ColumnMappingsDocument> mappings) {
    ObjectTypeColumnMappings typeColumnMappings =
        delegate.buildObjectTypeColumnMappings(objectType.getFieldsMetaMap().keySet(), mappings);
    var newTypeBldr = objectType.toBuilder();
    newTypeBldr.clearFieldsMeta().clearColumnMappingMeta();
    newTypeBldr.setColumnMappingMeta(typeColumnMappings.getColumnMappingMeta());
    var internalFieldsMetaMap = typeColumnMappings.getFieldsMetaMap();
    newTypeBldr.putAllFieldsMeta(toEntityFieldMetadataMap(internalFieldsMetaMap));
    return newTypeBldr.build();
  }
}
