package ai.traceable.fraud.datamodel.config.service.column.mapping;

import ai.traceable.fraud.datamodel.config.service.v1.MetricType;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeColumnMappings;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.List;

public class MetricTypeColumnMapper implements ColumnMapper<MetricType> {
  private final ColumnMapperDelegate delegate;

  @Inject
  protected MetricTypeColumnMapper(ColumnMapperDelegate delegate) {
    this.delegate = delegate;
  }

  @Override
  public ObjectTypeColumnMappings createColumnMapping(MetricType newType) {
    return ObjectTypeColumnMappings.newBuilder()
        .putAllFieldsMeta(newType.getFieldsMetaMap())
        // these are not allowed to change, so if user updates it this will undo that
        // todo: add default mappings here.
        .build();
  }

  @Override
  public MetricType forCreate(String tenantId, MetricType inputType) throws IOException {
    String typeId = inputType.getId();
    MetricType.Builder newTypeBldr = inputType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            tenantId, ObjectKind.OBJECT_KIND_METRIC, typeId, createColumnMapping(inputType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public MetricType forUpdate(String tenantId, MetricType currType, MetricType newType)
      throws IOException {
    // assumes that the type validator is invoked beforehand
    String typeId = newType.getId();
    MetricType.Builder newTypeBldr = newType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            tenantId, ObjectKind.OBJECT_KIND_METRIC, typeId, createColumnMapping(newType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public MetricType populateFieldMappings(
      MetricType objectType, List<ColumnMappingsDocument> mappings) {
    ObjectTypeColumnMappings typeColumnMappings = delegate.buildObjectTypeColumnMappings(mappings);
    var newTypeBldr = objectType.toBuilder();
    newTypeBldr.clearFieldsMeta().clearColumnMappingMeta();
    newTypeBldr.setColumnMappingMeta(typeColumnMappings.getColumnMappingMeta());
    newTypeBldr.putAllFieldsMeta(typeColumnMappings.getFieldsMetaMap());
    return newTypeBldr.build();
  }
}
