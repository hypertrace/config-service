package ai.traceable.fraud.datamodel.config.service.column.mapping;

import static ai.traceable.fraud.datamodel.config.service.FraudDataModelConstants.DEFAULT_FIELD_MAP;
import static ai.traceable.fraud.datamodel.config.service.FraudDataModelConstants.GENERIC_METRICS_FIELD_MAP;
import static ai.traceable.fraud.datamodel.config.service.FraudDataModelConstants.getColumnName;
import static ai.traceable.fraud.datamodel.config.service.FraudDataModelConstants.getKeyPrefix;

import ai.traceable.fraud.datamodel.config.service.FraudDataModelUtils;
import ai.traceable.fraud.datamodel.config.service.v1.ColumnMapping;
import ai.traceable.fraud.datamodel.config.service.v1.ColumnMappingMeta;
import ai.traceable.fraud.datamodel.config.service.v1.InternalFieldMetadata;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectTypeColumnMappings;
import com.google.inject.Inject;
import io.grpc.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.model.exception.DuplicateDocumentException;
import org.hypertrace.core.grpcutils.context.ContextualStatusExceptionBuilder;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ColumnMapperDelegateImpl implements ColumnMapperDelegate {
  private final ColumnMappingsStore columnMappingsStore;

  @Inject
  public ColumnMapperDelegateImpl(ColumnMappingsStore columnMappingsStore) {
    this.columnMappingsStore = columnMappingsStore;
  }

  @Override
  public List<ColumnMappingsDocument> mapProperties(
      RequestContext requestContext,
      ObjectKind kind,
      String typeId,
      ObjectTypeColumnMappings fields)
      throws IOException {
    int attempts = 0;
    String tenantId = FraudDataModelUtils.getTenantId(requestContext);
    while (attempts++ <= 10) {
      try {
        return doMapProperties(requestContext, kind, typeId, fields);
      } catch (DuplicateDocumentException e) {
        log.warn(
            "There was a duplicate key conflict while mapping for ["
                + typeId
                + "] for properties "
                + fields
                + ". The mapping will be attempted again");
      }
    }
    throw ContextualStatusExceptionBuilder.from(
            Status.INTERNAL.withDescription(
                "All attempts to create mapping for tenant: "
                    + tenantId
                    + ", type:"
                    + typeId
                    + " exhausted"))
        .useStatusDescriptionAsExternalMessage()
        .buildRuntimeException();
  }

  @Override
  public ObjectTypeColumnMappings buildObjectTypeColumnMappings(
      Set<String> fields, List<ColumnMappingsDocument> mappings) {
    ObjectTypeColumnMappings.Builder builder = ObjectTypeColumnMappings.newBuilder();
    ColumnMappingMeta.Builder columnMappingBuilder = ColumnMappingMeta.newBuilder();
    for (ColumnMappingsDocument mapping : mappings) {
      InternalFieldMetadata fieldMeta = mapping.getInternalFieldMetadata();
      String fieldName = mapping.getFieldName();
      if (!fields.contains(fieldName)) {
        continue;
      }
      builder.putFieldsMeta(mapping.getFieldName(), fieldMeta);
      if (!fieldMeta.getReserved()
          && !columnMappingBuilder.getColumnMappingMap().containsKey(mapping.getFieldName())) {
        columnMappingBuilder.putColumnMapping(
            mapping.getFieldName(),
            ColumnMapping.newBuilder()
                .setColumnId(mapping.getColumnId())
                .setFieldType(fieldMeta.getFieldType())
                .build());
        columnMappingBuilder.putRevColumnMapping(mapping.getColumnId(), mapping.getFieldName());
      }
    }
    return builder.setColumnMappingMeta(columnMappingBuilder).build();
  }

  @Override
  public void deleteColumnMappings(RequestContext requestContext, ObjectKind kind, String typeId)
      throws IOException {
    columnMappingsStore.deleteColumnMappings(requestContext, kind, typeId);
  }

  private List<ColumnMappingsDocument> doMapProperties(
      RequestContext requestContext,
      ObjectKind kind,
      String typeId,
      ObjectTypeColumnMappings fields)
      throws IOException {
    List<ColumnMappingsDocument> currMappings =
        columnMappingsStore.getColumnMappings(requestContext, kind, typeId);
    Map<String, ColumnMappingsDocument> fieldMap = new HashMap<>();
    Map<String, ColumnMappingsDocument> colMap = new HashMap<>();
    for (ColumnMappingsDocument currMapping : currMappings) {
      fieldMap.put(currMapping.getFieldName(), currMapping);
      colMap.put(currMapping.getColumnId(), currMapping);
    }
    List<ColumnMappingsDocument> newMappings = new ArrayList<>();
    for (Map.Entry<String, InternalFieldMetadata> entry : fields.getFieldsMetaMap().entrySet()) {
      String fieldName = entry.getKey();
      // first, check if this key is already mapped
      if (fieldMap.containsKey(fieldName)) {
        continue;
      }
      InternalFieldMetadata fieldMeta = entry.getValue();
      ColumnMappingsDocument columnMappingsDocument =
          buildObjectTypeColumnMappings(requestContext, kind, typeId, fieldName, fieldMeta, colMap);
      newMappings.add(columnMappingsDocument);
    }
    if (!newMappings.isEmpty()) {
      columnMappingsStore.addColumnMappings(requestContext, newMappings);
    }
    return columnMappingsStore.getColumnMappings(requestContext, kind, typeId);
  }

  private ColumnMappingsDocument buildObjectTypeColumnMappings(
      RequestContext requestContext,
      ObjectKind objectKind,
      String typeId,
      String propName,
      InternalFieldMetadata fieldMeta,
      Map<String, ColumnMappingsDocument> colMap) {
    Map<String, Integer> columnIndexMap = getColumnIndexMap(objectKind);
    String colName = createNewMapping(propName, colMap, fieldMeta, typeId, columnIndexMap);
    String tenantId = FraudDataModelUtils.getTenantId(requestContext);
    ColumnMappingsDocument columnMappingsDocument =
        new ColumnMappingsDocument(tenantId, objectKind, typeId, propName, colName, fieldMeta);
    colMap.put(colName, columnMappingsDocument);
    return columnMappingsDocument;
  }

  private Map<String, Integer> getColumnIndexMap(ObjectKind objectKind) {
    if (Objects.requireNonNull(objectKind) == ObjectKind.OBJECT_KIND_METRIC) {
      return GENERIC_METRICS_FIELD_MAP;
    }
    return DEFAULT_FIELD_MAP;
  }

  private String createNewMapping(
      String propName,
      Map<String, ColumnMappingsDocument> colMap,
      InternalFieldMetadata fieldMetadata,
      String typeId,
      Map<String, Integer> fieldCountMap) {
    if (fieldMetadata.getReserved()) {
      return propName;
    }
    String keyPrefix = getKeyPrefix(fieldMetadata);
    Integer indexSize = fieldCountMap.get(keyPrefix);
    if (indexSize == null) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INTERNAL.withDescription(
                  "No column index size found for field_name: "
                      + propName
                      + " for field_type: "
                      + fieldMetadata.getFieldType()
                      + " for type_id: "
                      + typeId
                      + " for metadata: "
                      + fieldMetadata))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
    for (int i = 0; i < indexSize; i++) {
      String possibleKey = getColumnName(fieldMetadata, i);
      if (!colMap.containsKey(possibleKey)) {
        return possibleKey;
      }
    }
    throw ContextualStatusExceptionBuilder.from(
            Status.INTERNAL.withDescription(
                "Column limit reached for "
                    + fieldMetadata.getFieldType()
                    + " for type "
                    + typeId
                    + " while mapping the property "
                    + fieldMetadata))
        .useStatusDescriptionAsExternalMessage()
        .buildRuntimeException();
  }
}
