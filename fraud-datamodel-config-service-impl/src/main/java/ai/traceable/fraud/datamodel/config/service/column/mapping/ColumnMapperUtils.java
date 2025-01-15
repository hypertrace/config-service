package ai.traceable.fraud.datamodel.config.service.column.mapping;

import ai.traceable.fraud.datamodel.config.service.v1.EntityFieldMetadata;
import ai.traceable.fraud.datamodel.config.service.v1.FieldMetadata;
import ai.traceable.fraud.datamodel.config.service.v1.InternalFieldMetadata;
import java.util.HashMap;
import java.util.Map;

class ColumnMapperUtils {
  static Map<String, InternalFieldMetadata> entityFieldMetaToInternalFieldMetadataMap(
      Map<String, EntityFieldMetadata> entityFieldMetadataMap) {
    Map<String, InternalFieldMetadata> result = new HashMap<>(entityFieldMetadataMap.size());
    for (var entry : entityFieldMetadataMap.entrySet()) {
      result.put(entry.getKey(), entityFieldMetaToInternalFieldMetadata(entry.getValue()));
    }
    return result;
  }

  static Map<String, EntityFieldMetadata> toEntityFieldMetadataMap(
      Map<String, InternalFieldMetadata> internalFieldMetadataMap) {
    Map<String, EntityFieldMetadata> result = new HashMap<>(internalFieldMetadataMap.size());
    for (var entry : internalFieldMetadataMap.entrySet()) {
      result.put(entry.getKey(), toEntityFieldMetadata(entry.getValue()));
    }
    return result;
  }

  static InternalFieldMetadata entityFieldMetaToInternalFieldMetadata(
      EntityFieldMetadata entityFieldMetadata) {
    return InternalFieldMetadata.newBuilder()
        .setFieldType(entityFieldMetadata.getFieldType())
        .setVersioned(entityFieldMetadata.getVersioned())
        .setDisabled(entityFieldMetadata.getDisabled())
        .build();
  }

  static EntityFieldMetadata toEntityFieldMetadata(InternalFieldMetadata internalFieldMetadata) {
    return EntityFieldMetadata.newBuilder()
        .setFieldType(internalFieldMetadata.getFieldType())
        .setVersioned(internalFieldMetadata.getVersioned())
        .setDisabled(internalFieldMetadata.getDisabled())
        .build();
  }

  static Map<String, InternalFieldMetadata> fieldMetaToInternalFieldMetadataMap(
      Map<String, FieldMetadata> fieldMetadataMap) {
    Map<String, InternalFieldMetadata> result = new HashMap<>(fieldMetadataMap.size());
    for (var entry : fieldMetadataMap.entrySet()) {
      result.put(entry.getKey(), fieldMetaToInternalFieldMetadata(entry.getValue()));
    }
    return result;
  }

  static Map<String, FieldMetadata> toFieldMetadataMap(
      Map<String, InternalFieldMetadata> internalFieldMetadataMap) {
    Map<String, FieldMetadata> result = new HashMap<>(internalFieldMetadataMap.size());
    for (var entry : internalFieldMetadataMap.entrySet()) {
      result.put(entry.getKey(), toFieldMetadata(entry.getValue()));
    }
    return result;
  }

  static InternalFieldMetadata fieldMetaToInternalFieldMetadata(FieldMetadata fieldMetadata) {
    return InternalFieldMetadata.newBuilder()
        .setFieldType(fieldMetadata.getFieldType())
        .setVersioned(fieldMetadata.getVersioned())
        .setDisabled(fieldMetadata.getDisabled())
        .setEntityType(fieldMetadata.getEntityType())
        .setRelationshipType(fieldMetadata.getRelationshipType())
        .setIndexed(fieldMetadata.getIndexed())
        .build();
  }

  static FieldMetadata toFieldMetadata(InternalFieldMetadata internalFieldMetadata) {
    return FieldMetadata.newBuilder()
        .setFieldType(internalFieldMetadata.getFieldType())
        .setVersioned(internalFieldMetadata.getVersioned())
        .setDisabled(internalFieldMetadata.getDisabled())
        .setEntityType(internalFieldMetadata.getEntityType())
        .setRelationshipType(internalFieldMetadata.getRelationshipType())
        .setIndexed(internalFieldMetadata.getIndexed())
        .build();
  }
}
