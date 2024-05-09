package ai.traceable.fraud.datamodel.config.service.column.mapping;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.fraud.datamodel.config.service.v1.FieldMetadata;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ColumnMappingsStoreTest {
  private ColumnMappingsDocumentStore target;

  private Datastore datastore;
  private Collection collection;
  private Map<Key, Document> localStore;

  @BeforeEach
  public void setup() {
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
    target = new ColumnMappingsDocumentStore(datastore);
  }

  @Test
  public void testAddMappings() throws IOException {
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
              FieldMetadata.getDefaultInstance());
      mappings.add(mapping);
    }
    target.addColumnMappings(tenantId, mappings);

    // query them now.
    var outputMappings = target.getColumnMappings(tenantId, kind, typeId);
    Assertions.assertEquals(mappings.size(), outputMappings.size());
  }
}
