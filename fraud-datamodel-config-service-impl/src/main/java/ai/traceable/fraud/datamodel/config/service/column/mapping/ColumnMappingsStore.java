package ai.traceable.fraud.datamodel.config.service.column.mapping;

import ai.traceable.fraud.datamodel.config.service.v1.ObjectKind;
import java.io.IOException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ColumnMappingsStore {
  void addColumnMappings(RequestContext requestContext, List<ColumnMappingsDocument> mappings);

  List<ColumnMappingsDocument> getColumnMappings(
      RequestContext requestContext, ObjectKind objectKind, String objectTypeId) throws IOException;

  List<ColumnMappingsDocument> getColumnMappings(
      RequestContext requestContext,
      ObjectKind objectKind,
      String objectTypeId,
      List<String> fieldNames)
      throws IOException;

  List<ColumnMappingsDocument> deleteColumnMappings(
      RequestContext requestContext, ObjectKind objectKind, String objectTypeId) throws IOException;
}
