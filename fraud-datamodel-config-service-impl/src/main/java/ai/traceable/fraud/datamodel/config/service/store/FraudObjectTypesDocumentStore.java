package ai.traceable.fraud.datamodel.config.service.store;

import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;

import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectType;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeReference;
import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
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

public class FraudObjectTypesDocumentStore implements FraudObjectTypesStore {
  public static final String FRAUD_OBJECT_TYPES = "fraud_object_types";
  private final Collection collection;

  public FraudObjectTypesDocumentStore(Datastore datastore) {
    this.collection = datastore.getCollection(FRAUD_OBJECT_TYPES);
  }

  private ObjectType toObjectType(Document doc) throws IOException {
    var fraudObjectType = FraudObjectTypeDocument.fromJson(doc.toJson());
    return fraudObjectType.getObjectType();
  }

  @Override
  public Optional<ObjectType> getObjectType(
      String tenantId, ObjectTypeReference objectTypeReference) throws Exception {
    org.hypertrace.core.documentstore.query.Query query = getQuery(tenantId, objectTypeReference);
    try (CloseableIterator<Document> it = collection.aggregate(query)) {
      List<Document> list = ImmutableList.copyOf(it);
      if (list.size() == 1) {
        return Optional.of(toObjectType(list.get(0)));
      } else if (list.size() > 1) {
        throw Status.INTERNAL
            .withDescription(
                "More than 1 FraudObjectType documents returned for "
                    + objectTypeReference
                    + " for tenant:"
                    + tenantId)
            .asRuntimeException();
      }
    }
    return Optional.empty();
  }

  private static Query getQuery(String tenantId, ObjectTypeReference objectTypeReference) {
    return Query.builder()
        .setFilter(
            LogicalExpression.builder()
                .operator(LogicalOperator.AND)
                .operand(
                    RelationalExpression.of(
                        IdentifierExpression.of(FraudObjectTypeDocument.TENANT_ID_FIELD_NAME),
                        EQ,
                        ConstantExpression.of(tenantId)))
                .operand(
                    RelationalExpression.of(
                        IdentifierExpression.of(FraudObjectTypeDocument.OBJECT_KIND_FIELD_NAME),
                        EQ,
                        ConstantExpression.of(objectTypeReference.getObjectKind().name())))
                .operand(
                    RelationalExpression.of(
                        IdentifierExpression.of(FraudObjectTypeDocument.OBJECT_TYPE_ID_FIELD_NAME),
                        EQ,
                        ConstantExpression.of(objectTypeReference.getId())))
                .build())
        .build();
  }

  @Override
  public List<ObjectType> getAllObjectTypes(String tenantId, ObjectKind objectKind)
      throws Exception {
    Query.QueryBuilder queryBuilder = Query.builder();

    if (objectKind != null
        && objectKind != ObjectKind.OBJECT_KIND_UNSPECIFIED
        && objectKind != ObjectKind.UNRECOGNIZED) {
      queryBuilder.setFilter(
          LogicalExpression.builder()
              .operator(LogicalOperator.AND)
              .operand(buildTenantIdFilter(tenantId))
              .operand(
                  RelationalExpression.of(
                      IdentifierExpression.of(FraudObjectTypeDocument.TENANT_ID_FIELD_NAME),
                      EQ,
                      ConstantExpression.of(tenantId)))
              .build());
    } else {
      queryBuilder.setFilter(buildTenantIdFilter(tenantId));
    }
    try (CloseableIterator<Document> it = collection.aggregate(queryBuilder.build())) {
      List<ObjectType> results = new ArrayList<>();
      while (it.hasNext()) {
        var doc = it.next();
        results.add(toObjectType(doc));
      }
      return results;
    }
  }

  @Override
  public void putObjectTypes(String tenantId, List<ObjectType> objectTypes) throws Exception {
    Map<Key, Document> documentMap = new HashMap<>(objectTypes.size());
    var timestamp = System.currentTimeMillis();
    for (var objectType : objectTypes) {
      var typeRef = Utils.getObjectTypeReference(objectType);
      var key = new FraudObjectTypeKey(tenantId, typeRef);
      var doc =
          new FraudObjectTypeDocument(
              tenantId, typeRef.getObjectKind(), typeRef.getId(), timestamp, timestamp, objectType);
      documentMap.put(key, doc);
    }
    this.collection.bulkUpsert(documentMap);
  }

  @Override
  public void deleteObjectTypes(String tenantId, List<ObjectTypeReference> objectTypeReferences) {
    this.collection.delete(
        objectTypeReferences.stream()
            .map(o -> new FraudObjectTypeKey(tenantId, o))
            .collect(Collectors.toSet()));
  }

  private RelationalExpression buildTenantIdFilter(String tenantId) {
    return RelationalExpression.of(
        IdentifierExpression.of(FraudObjectTypeDocument.TENANT_ID_FIELD_NAME),
        EQ,
        ConstantExpression.of(tenantId));
  }
}
