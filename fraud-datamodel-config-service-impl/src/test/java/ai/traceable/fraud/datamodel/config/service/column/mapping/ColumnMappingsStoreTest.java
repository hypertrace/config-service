package ai.traceable.fraud.datamodel.config.service.column.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import ai.traceable.config.proto.utils.ResourceUtils;
import ai.traceable.fraud.datamodel.config.service.v1.FieldType;
import ai.traceable.fraud.datamodel.config.service.v1.InternalFieldMetadata;
import ai.traceable.fraud.datamodel.config.service.v1.MetricType;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectTypeColumnMappings;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;

public class ColumnMappingsStoreTest {
  private ColumnMappingsDocumentStore target;

  private Datastore datastore;
  private Collection collection;
  private Map<Key, Document> localStore;
  private ColumnMapperDelegate columnMapperDelegate;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);
    localStore = new HashMap<>();
    datastore = mock(Datastore.class);
    collection = mock(Collection.class);
    doAnswer(
            invocationOnMock -> {
              var documentMap = invocationOnMock.getArgument(0, Map.class);
              localStore.putAll(documentMap);
              return null;
            })
        .when(collection)
        .bulkUpsert(anyMap());
    doAnswer(
            invocationOnMock -> {
              var query = invocationOnMock.getArgument(0, Query.class);
              var iterator = localStore.values().iterator();
              return new CloseableIterator<Document>() {

                @Override
                public boolean hasNext() {
                  return iterator.hasNext();
                }

                @Override
                public Document next() {
                  return iterator.next();
                }

                @Override
                public void close() throws IOException {}
              };
            })
        .when(collection)
        .aggregate(any(Query.class));
    doReturn(collection).when(datastore).getCollection(anyString());
    target = spy(new ColumnMappingsDocumentStore(datastore));

    // Initialize ColumnMapperDelegate for delegate tests
    columnMapperDelegate = new ColumnMapperDelegateImpl(target);
  }

  @Test
  void testAddMappings() throws IOException {
    List<ColumnMappingsDocument> mappings = new ArrayList<>();
    var tenantId = "tenant1";
    var kind = ObjectKind.OBJECT_KIND_ENTITY;
    var typeId = "entityType";
    for (int ii = 0; ii < 10; ii++) {
      var mapping =
          new ColumnMappingsDocument(
              tenantId,
              kind,
              typeId,
              "field" + ii,
              "columnId" + ii,
              InternalFieldMetadata.getDefaultInstance());
      mappings.add(mapping);
    }
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    target.addColumnMappings(requestContext, mappings);

    // query them now.
    var outputMappings =
        RequestContext.forTenantId(tenantId)
            .call(() -> target.getColumnMappings(requestContext, kind, typeId));
    Assertions.assertEquals(mappings.size(), outputMappings.size());
  }

  @Test
  void testCreateMetricType_WithUnsupportedFields() throws IOException {
    var tenantId = "tenant1";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ColumnMapperDelegate columnMapperDelegate = new ColumnMapperDelegateImpl(target);
    MetricTypeColumnMapper metricTypeColumnMapper =
        new MetricTypeColumnMapper(columnMapperDelegate);
    MetricType metricType1 =
        ResourceUtils.readProto("fraud/datamodel/test_metric_type_1.json", MetricType.newBuilder())
            .build();
    try {
      metricTypeColumnMapper.forCreate(requestContext, metricType1);
      fail("Exception should have being thrown");
    } catch (Exception e) {
      assertInstanceOf(StatusRuntimeException.class, e);
    }
  }

  // Tests for ColumnMapperDelegate functionality
  @Test
  void testAddNewFieldMappings_Delegate() throws IOException {
    var tenantId = "test-tenant";
    var typeId = "test-type";
    var objectKind = ObjectKind.OBJECT_KIND_ENTITY;
    RequestContext requestContext = RequestContext.forTenantId(tenantId);

    // Setup existing mappings
    List<ColumnMappingsDocument> existingMappings = new ArrayList<>();
    existingMappings.add(
        createMapping(tenantId, objectKind, typeId, "field1", "col_1", FieldType.FIELD_TYPE_STR));

    doReturn(existingMappings).when(target).getColumnMappings(requestContext, objectKind, typeId);

    // Create new fields to add
    Map<String, InternalFieldMetadata> newFields = new HashMap<>();
    newFields.put("field1", createFieldMetadata(FieldType.FIELD_TYPE_STR));
    newFields.put("field2", createFieldMetadata(FieldType.FIELD_TYPE_INT));

    ObjectTypeColumnMappings newMappings =
        ObjectTypeColumnMappings.newBuilder().putAllFieldsMeta(newFields).build();

    // Execute
    columnMapperDelegate.mapProperties(requestContext, objectKind, typeId, newMappings);

    // Verify that only field2 was added (field1 already exists)
    ArgumentCaptor<List<ColumnMappingsDocument>> captor = ArgumentCaptor.forClass(List.class);
    verify(target).addColumnMappings(eq(requestContext), captor.capture());

    List<ColumnMappingsDocument> addedMappings = captor.getValue();
    assertEquals(1, addedMappings.size());
    assertEquals("field2", addedMappings.get(0).getFieldName());
  }

  @Test
  void testUpdateExistingFieldMappings_Delegate() throws IOException {
    var tenantId = "test-tenant";
    var typeId = "test-type";
    var objectKind = ObjectKind.OBJECT_KIND_ENTITY;
    RequestContext requestContext = RequestContext.forTenantId(tenantId);

    // Setup existing mappings with old metadata
    List<ColumnMappingsDocument> existingMappings = new ArrayList<>();
    existingMappings.add(
        createMapping(tenantId, objectKind, typeId, "field1", "col_1", FieldType.FIELD_TYPE_STR));
    existingMappings.add(
        createMapping(tenantId, objectKind, typeId, "field2", "col_2", FieldType.FIELD_TYPE_INT));

    doReturn(existingMappings).when(target).getColumnMappings(requestContext, objectKind, typeId);

    // Update field2 metadata (change type)
    Map<String, InternalFieldMetadata> updatedFields = new HashMap<>();
    updatedFields.put("field1", createFieldMetadata(FieldType.FIELD_TYPE_STR));
    updatedFields.put("field2", createFieldMetadata(FieldType.FIELD_TYPE_DOUBLE)); // Changed type

    ObjectTypeColumnMappings updatedMappings =
        ObjectTypeColumnMappings.newBuilder().putAllFieldsMeta(updatedFields).build();

    // Execute
    columnMapperDelegate.mapProperties(requestContext, objectKind, typeId, updatedMappings);

    // Verify that field2 metadata was updated
    ArgumentCaptor<List<ColumnMappingsDocument>> captor = ArgumentCaptor.forClass(List.class);
    verify(target).addColumnMappings(eq(requestContext), captor.capture());

    List<ColumnMappingsDocument> updatedMappingsList = captor.getValue();
    assertEquals(1, updatedMappingsList.size(), "Should only update field2");
    assertTrue(
        updatedMappingsList.stream()
            .anyMatch(
                m ->
                    m.getFieldName().equals("field2")
                        && m.getInternalFieldMetadata().getFieldType()
                            == FieldType.FIELD_TYPE_DOUBLE));
  }

  @Test
  void testDeleteRemovedFieldMappings_Delegate() throws IOException {
    var tenantId = "test-tenant";
    var typeId = "test-type";
    var objectKind = ObjectKind.OBJECT_KIND_ENTITY;
    RequestContext requestContext = RequestContext.forTenantId(tenantId);

    // Setup existing mappings with 3 fields
    List<ColumnMappingsDocument> existingMappings = new ArrayList<>();
    existingMappings.add(
        createMapping(tenantId, objectKind, typeId, "field1", "col_1", FieldType.FIELD_TYPE_STR));
    existingMappings.add(
        createMapping(tenantId, objectKind, typeId, "field2", "col_2", FieldType.FIELD_TYPE_INT));
    existingMappings.add(
        createMapping(tenantId, objectKind, typeId, "field3", "col_3", FieldType.FIELD_TYPE_BOOL));

    doReturn(existingMappings).when(target).getColumnMappings(requestContext, objectKind, typeId);

    // New object type with only field1 and field3 (field2 removed)
    Map<String, InternalFieldMetadata> remainingFields = new HashMap<>();
    remainingFields.put("field1", createFieldMetadata(FieldType.FIELD_TYPE_STR));
    remainingFields.put("field3", createFieldMetadata(FieldType.FIELD_TYPE_BOOL));

    ObjectTypeColumnMappings remainingMappings =
        ObjectTypeColumnMappings.newBuilder().putAllFieldsMeta(remainingFields).build();

    // Execute
    columnMapperDelegate.mapProperties(requestContext, objectKind, typeId, remainingMappings);

    // Verify that field2 was deleted
    ArgumentCaptor<List<ColumnMappingsDocument>> deleteCaptor = ArgumentCaptor.forClass(List.class);
    verify(target).deleteColumnMappings(eq(requestContext), deleteCaptor.capture());

    List<ColumnMappingsDocument> deletedMappings = deleteCaptor.getValue();
    assertEquals(1, deletedMappings.size());
    assertEquals("field2", deletedMappings.get(0).getFieldName());
  }

  @Test
  void testComplexUpdate_AddUpdateDelete_Delegate() throws IOException {
    var tenantId = "test-tenant";
    var typeId = "test-type";
    var objectKind = ObjectKind.OBJECT_KIND_ENTITY;
    RequestContext requestContext = RequestContext.forTenantId(tenantId);

    // Setup existing mappings
    List<ColumnMappingsDocument> existingMappings = new ArrayList<>();
    existingMappings.add(
        createMapping(tenantId, objectKind, typeId, "field1", "col_1", FieldType.FIELD_TYPE_STR));
    existingMappings.add(
        createMapping(tenantId, objectKind, typeId, "field2", "col_2", FieldType.FIELD_TYPE_INT));
    existingMappings.add(
        createMapping(
            tenantId, objectKind, typeId, "fieldToDelete", "col_3", FieldType.FIELD_TYPE_BOOL));

    doReturn(existingMappings).when(target).getColumnMappings(requestContext, objectKind, typeId);

    // New object type with:
    // - field1 unchanged
    // - field2 with updated metadata
    // - fieldToDelete removed
    // - newField added
    Map<String, InternalFieldMetadata> newFields = new HashMap<>();
    newFields.put("field1", createFieldMetadata(FieldType.FIELD_TYPE_STR));
    newFields.put("field2", createFieldMetadata(FieldType.FIELD_TYPE_DOUBLE)); // Changed type
    newFields.put("newField", createFieldMetadata(FieldType.FIELD_TYPE_BINARY));

    ObjectTypeColumnMappings newMappings =
        ObjectTypeColumnMappings.newBuilder().putAllFieldsMeta(newFields).build();

    // Execute
    columnMapperDelegate.mapProperties(requestContext, objectKind, typeId, newMappings);

    // Verify deletions
    ArgumentCaptor<List<ColumnMappingsDocument>> deleteCaptor = ArgumentCaptor.forClass(List.class);
    verify(target).deleteColumnMappings(eq(requestContext), deleteCaptor.capture());
    assertEquals(1, deleteCaptor.getValue().size());
    assertEquals("fieldToDelete", deleteCaptor.getValue().get(0).getFieldName());

    // Verify upserts (both additions and updates in single call)
    ArgumentCaptor<List<ColumnMappingsDocument>> upsertCaptor = ArgumentCaptor.forClass(List.class);
    verify(target).addColumnMappings(eq(requestContext), upsertCaptor.capture());

    List<ColumnMappingsDocument> upsertedMappings = upsertCaptor.getValue();

    // Should have both the update and the new field in a single upsert call
    assertEquals(
        2, upsertedMappings.size(), "Should have 2 mappings: updated field2 and new newField");

    boolean hasUpdate = false;
    boolean hasNew = false;

    for (ColumnMappingsDocument doc : upsertedMappings) {
      if (doc.getFieldName().equals("field2")
          && doc.getInternalFieldMetadata().getFieldType() == FieldType.FIELD_TYPE_DOUBLE) {
        hasUpdate = true;
      }
      if (doc.getFieldName().equals("newField")) {
        hasNew = true;
      }
    }

    assertTrue(hasUpdate, "field2 should have been updated");
    assertTrue(hasNew, "newField should have been added");
  }

  // Helper methods
  private ColumnMappingsDocument createMapping(
      String tenantId,
      ObjectKind objectKind,
      String typeId,
      String fieldName,
      String columnId,
      FieldType fieldType) {
    return new ColumnMappingsDocument(
        tenantId, objectKind, typeId, fieldName, columnId, createFieldMetadata(fieldType));
  }

  private InternalFieldMetadata createFieldMetadata(FieldType fieldType) {
    return InternalFieldMetadata.newBuilder().setFieldType(fieldType).setReserved(false).build();
  }
}
