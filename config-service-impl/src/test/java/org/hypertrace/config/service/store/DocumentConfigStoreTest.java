package org.hypertrace.config.service.store;

import static org.hypertrace.config.service.TestUtils.CONTEXT1;
import static org.hypertrace.config.service.TestUtils.RESOURCE_NAME;
import static org.hypertrace.config.service.TestUtils.RESOURCE_NAMESPACE;
import static org.hypertrace.config.service.TestUtils.TENANT_ID;
import static org.hypertrace.config.service.TestUtils.getConfig1;
import static org.hypertrace.config.service.TestUtils.getConfig2;
import static org.hypertrace.config.service.TestUtils.getConfigResourceContext;
import static org.hypertrace.config.service.store.DocumentConfigStore.CONFIGURATIONS_COLLECTION;
import static org.hypertrace.config.service.store.DocumentConfigStore.DATA_STORE_TYPE;
import static org.hypertrace.config.service.store.DocumentConfigStore.DOC_STORE_CONFIG_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Value;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.config.service.ConfigResource;
import org.hypertrace.config.service.ConfigResourceContext;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse.UpsertedConfig;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class DocumentConfigStoreTest {

  private static final long CONFIG_VERSION = 5;
  private static final String USER_ID = "user1";
  private static final long TIMESTAMP1 = 100L;
  private static final long TIMESTAMP2 = 200L;
  private static final long TIMESTAMP3 = 300L;
  private static Value config1 = getConfig1();
  private static Value config2 = getConfig2();
  private static ConfigResourceContext configResourceContext = getConfigResourceContext();
  private static Collection collection;
  private DocumentConfigStore configStore;
  private Clock mockClock;

  @BeforeEach()
  void beforeEach() {
    collection = mock(Collection.class);
    String datastoreType = "MockDatastore";
    DatastoreProvider.register(datastoreType, MockDatastore.class);
    Map<String, Object> dataStoreConfig =
        Map.of(DATA_STORE_TYPE, datastoreType, datastoreType, Map.of());
    Map<String, Object> configMap = Map.of(DOC_STORE_CONFIG_KEY, dataStoreConfig);
    Config storeConfig = ConfigFactory.parseMap(configMap);
    this.mockClock = mock(Clock.class);
    this.configStore = new DocumentConfigStore(mockClock);
    this.configStore.init(storeConfig);
  }

  @Test
  void writeConfig() throws IOException {
    CloseableIteratorImpl iterator =
        new CloseableIteratorImpl(
            Collections.singletonList(
                getConfigDocument(
                    configResourceContext.getContext(),
                    CONFIG_VERSION,
                    config1,
                    TIMESTAMP1,
                    TIMESTAMP2)));
    when(collection.search(any(Query.class))).thenReturn(iterator);

    UpsertedConfig upsertedConfig =
        configStore.writeConfig(configResourceContext, USER_ID, config1);
    assertEquals(config1, upsertedConfig.getConfig());
    assertEquals(TIMESTAMP1, upsertedConfig.getCreationTimestamp());

    ArgumentCaptor<Key> keyCaptor = ArgumentCaptor.forClass(Key.class);
    ArgumentCaptor<Document> documentCaptor = ArgumentCaptor.forClass(Document.class);
    verify(collection, times(1)).upsert(keyCaptor.capture(), documentCaptor.capture());

    Key key = keyCaptor.getValue();
    Document document = documentCaptor.getValue();
    long newVersion = CONFIG_VERSION + 1;
    assertEquals(new ConfigDocumentKey(configResourceContext), key);
    assertEquals(
        getConfigDocument(
            configResourceContext.getContext(),
            newVersion,
            config1,
            TIMESTAMP1,
            ((ConfigDocument) document).getUpdateTimestamp()),
        document);
  }

  @Test
  void getConfigs() throws IOException {
    CloseableIteratorImpl iterator =
        new CloseableIteratorImpl(
            Collections.singletonList(
                getConfigDocument("context", CONFIG_VERSION, config1, TIMESTAMP1, TIMESTAMP2)));
    when(collection.search(any(Query.class))).thenReturn(iterator);

    ContextSpecificConfig expectedConfig =
        ContextSpecificConfig.newBuilder()
            .setConfig(config1)
            .setContext("context")
            .setCreationTimestamp(TIMESTAMP1)
            .setUpdateTimestamp(TIMESTAMP2)
            .build();
    Map<ConfigResourceContext, Optional<ContextSpecificConfig>> actualConfigs =
        configStore.getContextConfigs(Set.of(configResourceContext));
    assertEquals(Map.of(configResourceContext, Optional.of(expectedConfig)), actualConfigs);
  }

  @Test
  void getConfig() throws IOException {
    CloseableIteratorImpl iterator =
        new CloseableIteratorImpl(
            Collections.singletonList(
                getConfigDocument("context", CONFIG_VERSION, config1, TIMESTAMP1, TIMESTAMP2)));
    when(collection.search(any(Query.class))).thenReturn(iterator);

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
    when(collection.search(any(Query.class))).thenReturn(iterator);

    List<ContextSpecificConfig> contextSpecificConfigList =
        configStore.getAllConfigs(new ConfigResource(RESOURCE_NAME, RESOURCE_NAMESPACE, TENANT_ID));
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

    when(collection.search(any(Query.class))).thenReturn(getResult);
    when(collection.bulkUpsert(any())).thenReturn(true);
    when(this.mockClock.millis()).thenReturn(updateTime);
    List<UpsertedConfig> upsertedConfigs =
        // Swap configs between contexts as an update
        configStore.writeAllConfigs(
            ImmutableMap.of(resourceContext1, config2, resourceContext2, config1), USER_ID);

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

  private static Document getConfigDocument(
      String context, long version, Value config, long creationTimestamp, long updateTimestamp) {
    return new ConfigDocument(
        RESOURCE_NAME,
        RESOURCE_NAMESPACE,
        TENANT_ID,
        context,
        version,
        USER_ID,
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

  public static class MockDatastore implements Datastore {

    @Override
    public Set<String> listCollections() {
      return Collections.singleton(CONFIGURATIONS_COLLECTION);
    }

    @Override
    public Collection getCollection(String s) {
      return collection;
    }

    // default implementation for other methods as they are unused

    @Override
    public boolean init(Config config) {
      return false;
    }

    @Override
    public boolean createCollection(String s, Map<String, String> map) {
      return false;
    }

    @Override
    public boolean deleteCollection(String s) {
      return false;
    }

    @Override
    public boolean healthCheck() {
      return false;
    }
  }
}
