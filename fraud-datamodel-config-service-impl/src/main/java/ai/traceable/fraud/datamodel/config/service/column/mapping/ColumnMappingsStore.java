package ai.traceable.fraud.datamodel.config.service.column.mapping;

import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import java.io.IOException;
import java.util.List;

public interface ColumnMappingsStore {
  void addColumnMappings(String tenantId, List<ColumnMappingsDocument> mappings);

  List<ColumnMappingsDocument> getColumnMappings(
      String tenantId, ObjectKind objectKind, String objectTypeId) throws IOException;

  List<ColumnMappingsDocument> getColumnMappings(
      String tenantId, ObjectKind objectKind, String objectTypeId, List<String> fieldNames)
      throws IOException;
}
