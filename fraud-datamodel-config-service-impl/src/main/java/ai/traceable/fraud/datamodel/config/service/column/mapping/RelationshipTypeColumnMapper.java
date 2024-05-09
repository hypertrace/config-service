package ai.traceable.fraud.datamodel.config.service.column.mapping;

import ai.traceable.fraud.datamodel.config.service.v1.RelationshipType;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeColumnMappings;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.List;

public class RelationshipTypeColumnMapper implements ColumnMapper<RelationshipType> {

  private final ColumnMapperDelegate delegate;

  @Inject
  protected RelationshipTypeColumnMapper(ColumnMapperDelegate delegate) {
    this.delegate = delegate;
  }

  @Override
  public ObjectTypeColumnMappings createColumnMapping(RelationshipType newType) {
    return ObjectTypeColumnMappings.getDefaultInstance();
  }

  @Override
  public RelationshipType forCreate(String tenantId, RelationshipType inputType)
      throws IOException {
    String typeId = inputType.getId();
    RelationshipType.Builder newTypeBldr = inputType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            tenantId, ObjectKind.OBJECT_KIND_RELATIONSHIP, typeId, createColumnMapping(inputType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public RelationshipType forUpdate(
      String tenantId, RelationshipType currType, RelationshipType newType) throws IOException {
    // assumes that the type validator is invoked beforehand
    String typeId = newType.getId();
    RelationshipType.Builder newTypeBldr = newType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            tenantId, ObjectKind.OBJECT_KIND_RELATIONSHIP, typeId, createColumnMapping(newType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public RelationshipType populateFieldMappings(
      RelationshipType objectType, List<ColumnMappingsDocument> mappings) {
    // nothing to do
    return objectType;
  }
}
