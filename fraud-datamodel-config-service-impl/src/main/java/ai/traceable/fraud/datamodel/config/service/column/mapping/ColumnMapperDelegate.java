package ai.traceable.fraud.datamodel.config.service.column.mapping;

import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeColumnMappings;
import java.io.IOException;
import java.util.List;

public interface ColumnMapperDelegate {
  List<ColumnMappingsDocument> mapProperties(
      String tenantId, ObjectKind kind, String typeId, ObjectTypeColumnMappings fields)
      throws IOException;

  ObjectTypeColumnMappings buildObjectTypeColumnMappings(List<ColumnMappingsDocument> mappings);
}
