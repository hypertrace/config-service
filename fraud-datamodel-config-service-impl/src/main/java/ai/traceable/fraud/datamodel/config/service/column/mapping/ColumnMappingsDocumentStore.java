package ai.traceable.fraud.datamodel.config.service.column.mapping;

import static ai.traceable.fraud.datamodel.config.service.FraudDataModelUtils.getTenantId;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;

import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ColumnMappingsDocumentStore implements ColumnMappingsStore {
  public static final String FRAUD_OBJECT_TYPES_COLUMN_MAPPINGS =
      "fraud_object_types_column_mappings";
  private final Collection collection;

  public ColumnMappingsDocumentStore(Datastore datastore) {
    this.collection = datastore.getCollection(FRAUD_OBJECT_TYPES_COLUMN_MAPPINGS);
  }

  @Override
  public void addColumnMappings(
      RequestContext requestContext, List<ColumnMappingsDocument> mappings) {
    Map<Key, Document> documentMap = new HashMap<>(mappings.size());
    String tenantId = getTenantId(requestContext);
    for (ColumnMappingsDocument mapping : mappings) {
      ColumnMappingsKey key =
          new ColumnMappingsKey(
              tenantId,
              mapping.getObjectKind(),
              mapping.getObjectTypeId(),
              mapping.getFieldName(),
              mapping.getColumnId());
      ColumnMappingsDocument doc =
          new ColumnMappingsDocument(
              tenantId,
              mapping.getObjectKind(),
              mapping.getObjectTypeId(),
              mapping.getFieldName(),
              mapping.getColumnId(),
              mapping.getInternalFieldMetadata());
      documentMap.put(key, doc);
    }
    this.collection.bulkUpsert(documentMap);
  }

  @Override
  public List<ColumnMappingsDocument> getColumnMappings(
      RequestContext requestContext, ObjectKind objectKind, String objectTypeId)
      throws IOException {
    return getColumnMappingsInternal(
        requestContext, objectKind, objectTypeId, Collections.emptyList());
  }

  @Override
  public List<ColumnMappingsDocument> getColumnMappings(
      RequestContext requestContext,
      ObjectKind objectKind,
      String objectTypeId,
      List<String> fieldNames)
      throws IOException {
    return getColumnMappingsInternal(requestContext, objectKind, objectTypeId, fieldNames);
  }

  private List<ColumnMappingsDocument> getColumnMappingsInternal(
      RequestContext requestContext,
      ObjectKind objectKind,
      String objectTypeId,
      List<String> fieldNames)
      throws IOException {
    String tenantId = getTenantId(requestContext);
    LogicalExpression.LogicalExpressionBuilder filters =
        LogicalExpression.builder()
            .operator(LogicalOperator.AND)
            .operand(
                RelationalExpression.of(
                    IdentifierExpression.of(ColumnMappingsDocument.TENANT_ID_FIELD_NAME),
                    EQ,
                    ConstantExpression.of(tenantId)))
            .operand(
                RelationalExpression.of(
                    IdentifierExpression.of(ColumnMappingsDocument.OBJECT_TYPE_ID_FIELD_NAME),
                    EQ,
                    ConstantExpression.of(objectTypeId)))
            .operand(
                RelationalExpression.of(
                    IdentifierExpression.of(ColumnMappingsDocument.OBJECT_KIND_FIELD_NAME),
                    EQ,
                    ConstantExpression.of(objectKind.name())));
    if (!fieldNames.isEmpty()) {
      filters.operand(
          RelationalExpression.of(
              IdentifierExpression.of(ColumnMappingsDocument.FIELD_NAME),
              IN,
              ConstantExpression.ofStrings(fieldNames)));
    }
    Query.QueryBuilder queryBuilder = Query.builder().setFilter(filters.build());
    try (CloseableIterator<Document> it = collection.aggregate(queryBuilder.build())) {
      List<ColumnMappingsDocument> results = new ArrayList<>();
      while (it.hasNext()) {
        var doc = it.next();
        results.add(ColumnMappingsDocument.fromJson(doc.toJson()));
      }
      return results;
    }
  }
}
