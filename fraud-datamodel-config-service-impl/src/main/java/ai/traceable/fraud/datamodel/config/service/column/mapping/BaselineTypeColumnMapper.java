package ai.traceable.fraud.datamodel.config.service.column.mapping;

import static ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMapperUtils.fieldMetaToInternalFieldMetadataMap;
import static ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMapperUtils.toFieldMetadataMap;

import ai.traceable.fraud.datamodel.config.service.v1.BaselineType;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectTypeColumnMappings;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class BaselineTypeColumnMapper implements ColumnMapper<BaselineType> {
  private static final String BASELINE_COLUMN_MAPPINGS_DOC_TYPE_ID = "baseline_column_mappings";

  private final ColumnMapperDelegate delegate;

  @Inject
  protected BaselineTypeColumnMapper(ColumnMapperDelegate delegate) {
    this.delegate = delegate;
  }

  @Override
  public ObjectTypeColumnMappings createColumnMapping(BaselineType newType) {
    return ObjectTypeColumnMappings.newBuilder()
        .putAllFieldsMeta(fieldMetaToInternalFieldMetadataMap(newType.getFieldsMetaMap()))
        .build();
  }

  @Override
  public BaselineType forCreate(RequestContext requestContext, BaselineType inputType)
      throws IOException {
    BaselineType.Builder newTypeBldr = inputType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            requestContext,
            ObjectKind.OBJECT_KIND_BASELINE,
            BASELINE_COLUMN_MAPPINGS_DOC_TYPE_ID,
            createColumnMapping(inputType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public BaselineType forUpdate(
      RequestContext requestContext, BaselineType currType, BaselineType newType)
      throws IOException {
    // assumes that the type validator is invoked beforehand
    BaselineType.Builder newTypeBldr = newType.toBuilder();
    List<ColumnMappingsDocument> mappings =
        delegate.mapProperties(
            requestContext,
            ObjectKind.OBJECT_KIND_BASELINE,
            BASELINE_COLUMN_MAPPINGS_DOC_TYPE_ID,
            createColumnMapping(newType));
    return populateFieldMappings(newTypeBldr.build(), mappings);
  }

  @Override
  public BaselineType populateFieldMappings(
      BaselineType objectType, List<ColumnMappingsDocument> mappings) {
    ObjectTypeColumnMappings typeColumnMappings =
        delegate.buildObjectTypeColumnMappings(objectType.getFieldsMetaMap().keySet(), mappings);
    BaselineType.Builder newTypeBldr = objectType.toBuilder();
    newTypeBldr.clearFieldsMeta().clearColumnMappingMeta();
    newTypeBldr.setColumnMappingMeta(typeColumnMappings.getColumnMappingMeta());
    newTypeBldr.putAllFieldsMeta(toFieldMetadataMap(typeColumnMappings.getFieldsMetaMap()));
    return newTypeBldr.build();
  }
}
