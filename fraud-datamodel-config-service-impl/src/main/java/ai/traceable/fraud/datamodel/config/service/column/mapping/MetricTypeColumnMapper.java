package ai.traceable.fraud.datamodel.config.service.column.mapping;

import static ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMapperUtils.fieldMetaToInternalFieldMetadataMap;
import static ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMapperUtils.toFieldMetadataMap;

import ai.traceable.fraud.datamodel.config.service.v1.MetricType;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectTypeColumnMappings;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class MetricTypeColumnMapper implements ColumnMapper<MetricType> {
  // todo: deprecate this, use typeid level mappings just like other types. otherwise these are
  // tenant level mappings
  private static final String METRIC_COLUMN_MAPPINGS_DOC_TYPE_ID = "metric_column_mappings";

  private final ColumnMapperDelegate delegate;

  @Inject
  protected MetricTypeColumnMapper(ColumnMapperDelegate delegate) {
    this.delegate = delegate;
  }

  @Override
  public ObjectTypeColumnMappings createColumnMapping(MetricType newType) {
    return ObjectTypeColumnMappings.newBuilder()
        .putAllFieldsMeta(fieldMetaToInternalFieldMetadataMap(newType.getFieldsMetaMap()))
        .build();
  }

  @Override
  public MetricType forCreate(RequestContext requestContext, MetricType inputType)
      throws IOException {
    String typeId = inputType.getId();
    MetricType.Builder newTypeBldr = inputType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            requestContext,
            ObjectKind.OBJECT_KIND_METRIC,
            METRIC_COLUMN_MAPPINGS_DOC_TYPE_ID,
            createColumnMapping(inputType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public MetricType forUpdate(
      RequestContext requestContext, MetricType currType, MetricType newType) throws IOException {
    // assumes that the type validator is invoked beforehand
    String typeId = newType.getId();
    MetricType.Builder newTypeBldr = newType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            requestContext,
            ObjectKind.OBJECT_KIND_METRIC,
            METRIC_COLUMN_MAPPINGS_DOC_TYPE_ID,
            createColumnMapping(newType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public MetricType populateFieldMappings(
      MetricType objectType, List<ColumnMappingsDocument> mappings) {
    ObjectTypeColumnMappings typeColumnMappings =
        delegate.buildObjectTypeColumnMappings(objectType.getFieldsMetaMap().keySet(), mappings);
    var newTypeBldr = objectType.toBuilder();
    newTypeBldr.clearFieldsMeta().clearColumnMappingMeta();
    newTypeBldr.setColumnMappingMeta(typeColumnMappings.getColumnMappingMeta());
    newTypeBldr.putAllFieldsMeta(toFieldMetadataMap(typeColumnMappings.getFieldsMetaMap()));
    return newTypeBldr.build();
  }

  @Override
  public void deleteColumnMappings(RequestContext requestContext, MetricType objectType)
      throws IOException {
    // no op until we make mappings type specific
  }
}
