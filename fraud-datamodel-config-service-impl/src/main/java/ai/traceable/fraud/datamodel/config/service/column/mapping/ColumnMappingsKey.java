package ai.traceable.fraud.datamodel.config.service.column.mapping;

import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import lombok.Value;
import org.hypertrace.core.documentstore.Key;

@Value
public class ColumnMappingsKey implements Key {
  private static final String SEPARATOR = ":";

  String tenantId;
  ObjectKind objectKind;
  String objectTypeId;
  String fieldName;
  String columnId;

  @Override
  public String toString() {
    var key =
        String.join(SEPARATOR, this.tenantId, objectKind.name(), objectTypeId, fieldName, columnId);
    return UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8)).toString();
  }
}
