package ai.traceable.fraud.datamodel.config.service.column.mapping;

import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeColumnMappings;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.List;

public interface ColumnMapper<T extends Message> {
  ObjectTypeColumnMappings createColumnMapping(T newType);

  T forCreate(String tenantId, T inputType) throws IOException;

  T forUpdate(String tenantId, T currType, T newType) throws IOException;

  T populateFieldMappings(T objectType, List<ColumnMappingsDocument> mappings);
}
