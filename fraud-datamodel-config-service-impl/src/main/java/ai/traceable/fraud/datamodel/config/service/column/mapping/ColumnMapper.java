package ai.traceable.fraud.datamodel.config.service.column.mapping;

import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeColumnMappings;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ColumnMapper<T extends Message> {
  ObjectTypeColumnMappings createColumnMapping(T newType);

  T forCreate(RequestContext requestContext, T inputType) throws IOException;

  T forUpdate(RequestContext requestContext, T currType, T newType) throws IOException;

  T populateFieldMappings(T objectType, List<ColumnMappingsDocument> mappings);
}
