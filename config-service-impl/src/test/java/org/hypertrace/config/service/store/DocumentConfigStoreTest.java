package org.hypertrace.config.service.store;

import static java.util.Collections.emptyList;
import static org.hypertrace.config.service.TestUtils.CONTEXT1;
import static org.hypertrace.config.service.TestUtils.RESOURCE_NAME;
import static org.hypertrace.config.service.TestUtils.RESOURCE_NAMESPACE;
import static org.hypertrace.config.service.TestUtils.TENANT_ID;
import static org.hypertrace.config.service.TestUtils.getConfig1;
import static org.hypertrace.config.service.TestUtils.getConfig2;
import static org.hypertrace.config.service.TestUtils.getConfigResourceContext;
import static org.hypertrace.config.service.store.DocumentConfigStore.CONFIGURATIONS_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.config.service.ConfigResource;
import org.hypertrace.config.service.ConfigResourceContext;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.Filter;
import org.hypertrace.config.service.v1.LogicalFilter;
import org.hypertrace.config.service.v1.LogicalOperator;
import org.hypertrace.config.service.v1.Pagination;
import org.hypertrace.config.service.v1.RelationalFilter;
import org.hypertrace.config.service.v1.RelationalOperator;
import org.hypertrace.config.service.v1.SortOrder;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse.UpsertedConfig;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.UpdateResult;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.model.options.QueryOptions;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SortingSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DocumentConfigStoreTest {

  private static final long CONFIG_VERSION = 5;
  private static final String USER_ID = "user1";
  private static final String USER_EMAIL = "user@email.com";
  private static final long TIMESTAMP1 = 100L;
  private static final long TIMESTAMP2 = 200L;
  private static final long TIMESTAMP3 = 300L;
  private static Value config1 = getConfig1();
  private static Value config2 = getConfig2();
  private static ConfigResourceContext configResourceContext = getConfigResourceContext();

  private Collection collection;
  @Mock private Clock mockClock;
  @Mock private FilterBuilder filterBuilder;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Datastore mockDatastore;

  @InjectMocks private DocumentConfigStore configStore;

  @BeforeEach
  void beforeEach() {
    this.collection = mockDatastore.getCollection(CONFIGURATIONS_COLLECTION);
  }

  @Test
  void WriteConfigForCreate() throws IOException {
    CloseableIteratorImpl iterator = new CloseableIteratorImpl(List.of());
    when(collection.query(any(Query.class), any())).thenReturn(iterator);
    UpsertConfigRequest request = mock(UpsertConfigRequest.class);
    when(request.getConfig()).thenReturn(config1);
    when(request.hasUpsertCondition()).thenReturn(false);
    when(mockClock.millis()).thenReturn(TIMESTAMP1);
    UpsertedConfig upsertedConfig =
        configStore.writeConfig(configResourceContext, USER_ID, request, USER_EMAIL);
    assertEquals(config1, upsertedConfig.getConfig());
    assertEquals(TIMESTAMP1, upsertedConfig.getCreationTimestamp());

    ArgumentCaptor<Key> keyCaptor = ArgumentCaptor.forClass(Key.class);
    ArgumentCaptor<Document> documentCaptor = ArgumentCaptor.forClass(Document.class);
    verify(collection, times(1)).upsert(keyCaptor.capture(), documentCaptor.capture());

    Key key = keyCaptor.getValue();
    Document document = documentCaptor.getValue();
    assertEquals(new ConfigDocumentKey(configResourceContext), key);
    assertEquals(
        getConfigDocument(configResourceContext.getContext(), 1, config1, TIMESTAMP1, TIMESTAMP1),
        document);

    // throw exception when passed an upsert condition
    when(request.hasUpsertCondition()).thenReturn(true);
    assertThrows(
        StatusRuntimeException.class,
        () -> configStore.writeConfig(configResourceContext, USER_ID, request, USER_EMAIL));
  }

  @Test
  void WriteConfigForUpdateWithoutUpsertCondition() throws IOException {
    CloseableIteratorImpl iterator =
        new CloseableIteratorImpl(
            Collections.singletonList(
                getConfigDocument(
                    configResourceContext.getContext(),
                    CONFIG_VERSION,
                    config1,
                    TIMESTAMP1,
                    TIMESTAMP2)));
    when(collection.query(any(Query.class), any())).thenReturn(iterator);
    UpsertConfigRequest request = mock(UpsertConfigRequest.class);
    when(request.getConfig()).thenReturn(config1);
    when(request.hasUpsertCondition()).thenReturn(false);
    when(mockClock.millis()).thenReturn(TIMESTAMP2);
    UpsertedConfig upsertedConfig =
        configStore.writeConfig(configResourceContext, USER_ID, request, USER_EMAIL);
    assertEquals(config1, upsertedConfig.getConfig());
    assertEquals(TIMESTAMP1, upsertedConfig.getCreationTimestamp());
    assertEquals(TIMESTAMP2, upsertedConfig.getUpdateTimestamp());

    ArgumentCaptor<Key> keyCaptor = ArgumentCaptor.forClass(Key.class);
    ArgumentCaptor<Document> documentCaptor = ArgumentCaptor.forClass(Document.class);
    verify(collection, times(1)).upsert(keyCaptor.capture(), documentCaptor.capture());

    Key key = keyCaptor.getValue();
    Document document = documentCaptor.getValue();
    long newVersion = CONFIG_VERSION + 1;
    assertEquals(new ConfigDocumentKey(configResourceContext), key);
    assertEquals(
        getConfigDocument(
            configResourceContext.getContext(), newVersion, config1, TIMESTAMP1, TIMESTAMP2),
        document);
  }

  @Test
  void WriteConfigForUpdateWithUpsertCondition() throws IOException {
    CloseableIteratorImpl iterator =
        new CloseableIteratorImpl(
            Collections.singletonList(
                getConfigDocument(
                    configResourceContext.getContext(),
                    CONFIG_VERSION,
                    config1,
                    TIMESTAMP1,
                    TIMESTAMP2)));
    when(collection.query(any(Query.class), any())).thenReturn(iterator);
    UpsertConfigRequest request = mock(UpsertConfigRequest.class);
    UpdateResult updateResult = mock(UpdateResult.class);
    when(request.hasUpsertCondition()).thenReturn(true);
    when(request.getConfig()).thenReturn(config2);
    when(collection.update(any(Key.class), any(), any())).thenReturn(updateResult);
    when(updateResult.getUpdatedCount()).thenReturn(1L);

    Filter upsertCondition =
        Filter.newBuilder()
            .setLogicalFilter(
                LogicalFilter.newBuilder()
                    .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                    .addOperands(
                        Filter.newBuilder()
                            .setRelationalFilter(
                                RelationalFilter.newBuilder()
                                    .setConfigJsonPath("k1")
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                                    .setValue(Values.of(10))))
                    .addOperands(
                        Filter.newBuilder()
                            .setRelationalFilter(
                                RelationalFilter.newBuilder()
                                    .setConfigJsonPath("k2")
                                    .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                                    .setValue(Values.of("v2")))))
            .build();

    when(request.getUpsertCondition()).thenReturn(upsertCondition);
    UpsertedConfig upsertedConfig =
        configStore.writeConfig(configResourceContext, USER_ID, request, USER_EMAIL);
    assertEquals(config2, upsertedConfig.getConfig());
    assertEquals(TIMESTAMP1, upsertedConfig.getCreationTimestamp());

    ArgumentCaptor<Key> keyCaptor = ArgumentCaptor.forClass(Key.class);
    ArgumentCaptor<Document> documentCaptor = ArgumentCaptor.forClass(Document.class);
    ArgumentCaptor<org.hypertrace.core.documentstore.Filter> filterCaptor =
        ArgumentCaptor.forClass(org.hypertrace.core.documentstore.Filter.class);
    verify(collection, times(1))
        .update(keyCaptor.capture(), documentCaptor.capture(), filterCaptor.capture());

    Key key = keyCaptor.getValue();
    Document document = documentCaptor.getValue();
    long newVersion = CONFIG_VERSION + 1;
    assertEquals(new ConfigDocumentKey(configResourceContext), key);
    assertEquals(
        getConfigDocument(
            configResourceContext.getContext(),
            newVersion,
            config2,
            TIMESTAMP1,
            ((ConfigDocument) document).getUpdateTimestamp()),
        document);

    // failed upsert condition
    assertThrows(
        StatusRuntimeException.class,
        () -> configStore.writeConfig(configResourceContext, USER_ID, request, USER_EMAIL));
  }

  @Test
  void getConfigs() throws IOException {
    CloseableIteratorImpl iterator =
        new CloseableIteratorImpl(
            Collections.singletonList(
                getConfigDocument("context", CONFIG_VERSION, config1, TIMESTAMP1, TIMESTAMP2)));
    when(collection.query(any(Query.class), any())).thenReturn(iterator);

    ContextSpecificConfig expectedConfig =
        ContextSpecificConfig.newBuilder()
            .setConfig(config1)
            .setContext("context")
            .setCreationTimestamp(TIMESTAMP1)
            .setUpdateTimestamp(TIMESTAMP2)
            .build();
    ConfigResourceContext context = getConfigResourceContext("context");
    Map<ConfigResourceContext, ContextSpecificConfig> actualConfigs =
        configStore.getContextConfigs(Set.of(context));
    assertEquals(Map.of(context, expectedConfig), actualConfigs);
  }

  @Test
  void getConfig() throws IOException {
    CloseableIteratorImpl iterator =
        new CloseableIteratorImpl(
            Collections.singletonList(
                getConfigDocument("context", CONFIG_VERSION, config1, TIMESTAMP1, TIMESTAMP2)));
    when(collection.query(any(Query.class), any())).thenReturn(iterator);

    ContextSpecificConfig expectedConfig =
        ContextSpecificConfig.newBuilder()
            .setConfig(config1)
            .setContext("context")
            .setCreationTimestamp(TIMESTAMP1)
            .setUpdateTimestamp(TIMESTAMP2)
            .build();
    Optional<ContextSpecificConfig> actualConfig = configStore.getConfig(configResourceContext);
    assertEquals(Optional.of(expectedConfig), actualConfig);
  }

  @Test
  void getAllConfigs() throws IOException {
    CloseableIteratorImpl iterator =
        new CloseableIteratorImpl(
            List.of(
                getConfigDocument(CONTEXT1, 1L, config2, TIMESTAMP3, TIMESTAMP3),
                getConfigDocument("context", CONFIG_VERSION, config1, TIMESTAMP1, TIMESTAMP2),
                getConfigDocument("context", CONFIG_VERSION - 1, config2, TIMESTAMP1, TIMESTAMP1)));
    when(collection.query(any(Query.class), any())).thenReturn(iterator);

    List<ContextSpecificConfig> contextSpecificConfigList =
        configStore.getAllConfigs(
            new ConfigResource(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_ID),
            Filter.getDefaultInstance(), // for empty filter
            Pagination.getDefaultInstance(),
            emptyList());

    assertEquals(2, contextSpecificConfigList.size());
    assertEquals(
        ContextSpecificConfig.newBuilder()
            .setContext(CONTEXT1)
            .setConfig(config2)
            .setCreationTimestamp(TIMESTAMP3)
            .setUpdateTimestamp(TIMESTAMP3)
            .build(),
        contextSpecificConfigList.get(0));
    assertEquals(
        ContextSpecificConfig.newBuilder()
            .setContext("context")
            .setConfig(config1)
            .setCreationTimestamp(TIMESTAMP1)
            .setUpdateTimestamp(TIMESTAMP2)
            .build(),
        contextSpecificConfigList.get(1));
  }

  @Test
  void writeAllConfigs() throws IOException {
    ConfigResourceContext resourceContext1 = getConfigResourceContext("context-1");
    ConfigResourceContext resourceContext2 = getConfigResourceContext("context-2");
    long updateTime = 1234;
    CloseableIterator<Document> getResult =
        new CloseableIteratorImpl(
            List.of(
                getConfigDocument(
                    resourceContext1.getContext(), CONFIG_VERSION, config1, TIMESTAMP1, TIMESTAMP1),
                getConfigDocument(
                    resourceContext2.getContext(),
                    CONFIG_VERSION,
                    config2,
                    TIMESTAMP2,
                    TIMESTAMP2)));

    when(collection.query(any(Query.class), any())).thenReturn(getResult);
    when(collection.bulkUpsert(any())).thenReturn(true);
    when(this.mockClock.millis()).thenReturn(updateTime);
    List<UpsertedConfig> upsertedConfigs =
        // Swap configs between contexts as an update
        configStore.writeAllConfigs(
            ImmutableMap.of(resourceContext1, config2, resourceContext2, config1),
            USER_ID,
            USER_EMAIL);

    assertEquals(
        List.of(
            UpsertedConfig.newBuilder()
                .setContext(resourceContext1.getContext())
                .setCreationTimestamp(TIMESTAMP1)
                .setUpdateTimestamp(updateTime)
                .setConfig(config2)
                .setPrevConfig(config1)
                .build(),
            UpsertedConfig.newBuilder()
                .setContext(resourceContext2.getContext())
                .setCreationTimestamp(TIMESTAMP2)
                .setUpdateTimestamp(updateTime)
                .setConfig(config1)
                .setPrevConfig(config2)
                .build()),
        upsertedConfigs);

    verify(collection, times(1))
        .bulkUpsert(
            Map.of(
                new ConfigDocumentKey(resourceContext1),
                getConfigDocument(
                    resourceContext1.getContext(),
                    CONFIG_VERSION + 1,
                    config2,
                    TIMESTAMP1,
                    updateTime),
                new ConfigDocumentKey(resourceContext2),
                getConfigDocument(
                    resourceContext2.getContext(),
                    CONFIG_VERSION + 1,
                    config1,
                    TIMESTAMP2,
                    updateTime)));
  }

  @Test
  void buildQuery_withDefaultPagination() throws Exception {
    List<SortingSpec> expectedSorts =
        Arrays.asList(
            SortingSpec.of(
                IdentifierExpression.of("configVersion"),
                org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC),
            SortingSpec.of(
                IdentifierExpression.of("creationTimestamp"),
                org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC));

    configStore.getAllConfigs(
        new ConfigResource(RESOURCE_NAMESPACE, RESOURCE_NAME, "tenant1"),
        Filter.getDefaultInstance(),
        Pagination.getDefaultInstance(),
        emptyList());

    verify(collection)
        .query(argThat(query -> query.getSorts().equals(expectedSorts)), any(QueryOptions.class));
  }

  @Test
  void buildQuery_withCustomPagination() throws IOException {
    Pagination pagination = Pagination.newBuilder().setOffset(10).setLimit(20).build();
    org.hypertrace.core.documentstore.query.Pagination expectedPagination =
        org.hypertrace.core.documentstore.query.Pagination.builder().offset(10).limit(20).build();

    configStore.getAllConfigs(
        new ConfigResource(RESOURCE_NAMESPACE, RESOURCE_NAME, "tenant1"),
        Filter.getDefaultInstance(),
        pagination,
        emptyList());

    verify(collection)
        .query(
            argThat(query -> query.getPagination().orElseThrow().equals(expectedPagination)),
            any(QueryOptions.class));
  }

  @Test
  void buildQuery_withCustomSorting() throws IOException {
    org.hypertrace.config.service.v1.Selection selection =
        org.hypertrace.config.service.v1.Selection.newBuilder()
            .setConfigJsonPath("test.path")
            .build();
    org.hypertrace.config.service.v1.SortBy sortBy =
        org.hypertrace.config.service.v1.SortBy.newBuilder()
            .setSelection(selection)
            .setSortOrder(SortOrder.SORT_ORDER_DESC)
            .build();

    List<SortingSpec> expectedSorts =
        Arrays.asList(
            SortingSpec.of(
                IdentifierExpression.of("configVersion"),
                org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC),
            SortingSpec.of(
                IdentifierExpression.of("config.test.path"),
                org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC));

    configStore.getAllConfigs(
        new ConfigResource(RESOURCE_NAMESPACE, RESOURCE_NAME, "tenant1"),
        Filter.getDefaultInstance(),
        Pagination.getDefaultInstance(),
        Collections.singletonList(sortBy));

    verify(collection)
        .query(argThat(query -> query.getSorts().equals(expectedSorts)), any(QueryOptions.class));
  }

  @Test
  void buildQuery_withFilter() throws IOException {
    ConfigResource configResource =
        new ConfigResource(RESOURCE_NAMESPACE, RESOURCE_NAME, "tenant1");

    RelationalFilter relationalFilter =
        RelationalFilter.newBuilder()
            .setConfigJsonPath("test.path")
            .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
            .setValue(Values.of("test-value"))
            .build();

    Filter filter = Filter.newBuilder().setRelationalFilter(relationalFilter).build();

    Pagination pagination = Pagination.getDefaultInstance();
    List<org.hypertrace.config.service.v1.SortBy> sortByList = emptyList();

    // Create a mock for CloseableIterator<Document>
    CloseableIterator<Document> mockIterator = mock(CloseableIterator.class);
    when(mockIterator.hasNext()).thenReturn(false); // No documents to process

    // Mock the collection.query to return the mock iterator
    when(collection.query(any(Query.class), any(QueryOptions.class))).thenReturn(mockIterator);

    // Act
    configStore.getAllConfigs(configResource, filter, pagination, emptyList());

    // Capture the Query object passed to collection.query
    ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
    verify(collection).query(queryCaptor.capture(), any(QueryOptions.class));

    // Assert the properties of the captured Query
    Query capturedQuery = queryCaptor.getValue();
    assertNotNull(capturedQuery);

    // Verify filter is present in the query
    assertNotNull(capturedQuery.getFilter());

    // Compare the actual filter with the expected filter
    FilterTypeExpression actualFilter =
        ((LogicalExpression) capturedQuery.getFilter().get()).getOperands().get(1);
    FilterTypeExpression expectedFilter = createExpectedFilter();

    assertEquals(expectedFilter, actualFilter); // Compare the entire filter expressions
  }

  private FilterTypeExpression createExpectedFilter() {
    // Create the expected left-hand side and right-hand side expressions
    IdentifierExpression lhs = IdentifierExpression.of("config.test.path");
    ConstantExpression rhs = ConstantExpression.of("test-value");

    // Build the relational expression

    // Assuming you have a logical expression that contains this relational expression
    return RelationalExpression.of(
        lhs, org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ, rhs);
  }

  private Document getConfigDocument(
      String context, long version, Value config, long creationTimestamp, long updateTimestamp) {
    return new ConfigDocument(
        RESOURCE_NAME,
        RESOURCE_NAMESPACE,
        TENANT_ID,
        context,
        version,
        USER_ID,
        USER_EMAIL,
        config,
        creationTimestamp,
        updateTimestamp);
  }

  static class CloseableIteratorImpl implements CloseableIterator<Document> {
    Iterator<Document> documentIterator;

    public CloseableIteratorImpl(List<Document> documents) {
      documentIterator = documents.iterator();
    }

    @Override
    public void close() throws IOException {
      // nothing to do here
    }

    @Override
    public boolean hasNext() {
      return documentIterator.hasNext();
    }

    @Override
    public Document next() {
      return documentIterator.next();
    }
  }
}
