package ai.traceable.fraud.datamodel.config.service.column.mapping;

import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeColumnMappings;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ColumnMapperDelegate {
  List<ColumnMappingsDocument> mapProperties(
      RequestContext requestContext,
      ObjectKind kind,
      String typeId,
      ObjectTypeColumnMappings fields)
      throws IOException;

  ObjectTypeColumnMappings buildObjectTypeColumnMappings(
      Set<String> fields, List<ColumnMappingsDocument> mappings);
}
