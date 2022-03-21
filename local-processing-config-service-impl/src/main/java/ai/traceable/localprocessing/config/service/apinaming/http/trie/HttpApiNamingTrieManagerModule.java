package ai.traceable.localprocessing.config.service.apinaming.http.trie;

import ai.traceable.platform.apientity.http.difflog.TrieDiffLogModel;
import ai.traceable.platform.apientity.http.model.TrieModel;
import ai.traceable.platform.model.store.FileSystemModelStore;
import ai.traceable.platform.model.store.ModelPersistentStore;
import ai.traceable.platform.model.store.MultiFileSystemModelStore;
import ai.traceable.platform.model.store.config.MultiFileSystemModelStoreConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpApiNamingTrieManagerModule extends AbstractModule {

  private final Config config;

  private static final String MODEL_STORES_CONFIG_PATH = "model.stores";
  private static final String TRIE_MODEL_CONFIG_PATH = "api.naming.config.trieModels";
  private static final String MODEL_STORES_READ_ENABLED_FLAG = "read.enabled";
  private static final String MODEL_STORES_READ_PRIORITY_PATH = "read.priority";
  private static final String MODEL_STORES_WRITE_DISABLED_PATH = "write.disabled";
  private static final String TRIE_DIFF_LOG_MODEL_STORE_CONFIG_PATH =
      "api.naming.config.trieDiffLog.model.store";

  public HttpApiNamingTrieManagerModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(HttpApiNamingTrieManager.class).to(DefaultHttpApiNamingTrieManager.class);
  }

  @Provides
  @Singleton
  public ModelPersistentStore<TrieModel> provideTrieModelStore() {
    Config trieModelConfig = this.config.getConfig(TRIE_MODEL_CONFIG_PATH);
    ModelPersistentStore<TrieModel> modelStore =
        new MultiFileSystemModelStore<>(TrieModel.class, getModelStoreConfig(trieModelConfig));
    modelStore.init(trieModelConfig);
    return modelStore;
  }

  @Provides
  @Singleton
  public ModelPersistentStore<TrieDiffLogModel> provideTrieDiffLogModelStore() {
    ModelPersistentStore<TrieDiffLogModel> modelStore =
        new FileSystemModelStore<>(TrieDiffLogModel.class);
    Config trieDiffLogModelStoreConfig =
        this.config.getConfig(TRIE_DIFF_LOG_MODEL_STORE_CONFIG_PATH);
    modelStore.init(trieDiffLogModelStoreConfig);
    return modelStore;
  }

  private MultiFileSystemModelStoreConfig getModelStoreConfig(Config config) {
    List<? extends Config> stores = config.getConfigList(MODEL_STORES_CONFIG_PATH);
    MultiFileSystemModelStoreConfig.Builder configBuilder =
        MultiFileSystemModelStoreConfig.Builder.builder();
    log.info("building config for {} file store", stores.size());
    for (Config conf : stores) {
      configBuilder.addConfig(
          MultiFileSystemModelStoreConfig.FileSystemModelStoreConfig.Builder.builder()
              .withDeepStoreConfig(conf)
              .withReadPriority(
                  conf.hasPath(MODEL_STORES_READ_PRIORITY_PATH)
                      ? conf.getInt(MODEL_STORES_READ_PRIORITY_PATH)
                      : 1)
              .withReadEnabled(
                  conf.hasPath(MODEL_STORES_READ_ENABLED_FLAG)
                      && conf.getBoolean(MODEL_STORES_READ_ENABLED_FLAG))
              .withWriteDisabled(
                  conf.hasPath(MODEL_STORES_WRITE_DISABLED_PATH)
                      && conf.getBoolean(MODEL_STORES_WRITE_DISABLED_PATH))
              .build());
    }
    return configBuilder.build();
  }
}
