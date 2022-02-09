package ai.traceable.localprocessing.config.service.apinaming;

import ai.traceable.anomaly.config.service.v1.trainer.TrainerConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.config.ApiNamingConfig;
import ai.traceable.localprocessing.config.service.config.EntityDataServiceConfig;
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
import io.grpc.ManagedChannel;
import java.util.List;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiNamingManagerModule extends AbstractModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApiNamingManagerModule.class);

  private final Config config;

  private static final String MODEL_STORES_CONFIG_PATH = "model.stores";
  private static final String TRIE_MODEL_CONFIG_PATH = "api.naming.config.trieModels";
  private static final String MODEL_STORES_READ_ENABLED_FLAG = "read.enabled";
  private static final String MODEL_STORES_READ_PRIORITY_PATH = "read.priority";
  private static final String MODEL_STORES_WRITE_DISABLED_PATH = "write.disabled";
  private static final String TRIE_DIFF_LOG_MODEL_STORE_CONFIG_PATH =
      "api.naming.config.trieDiffLog.model.store";

  public ApiNamingManagerModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(ApiNamingManager.class).to(DefaultApiNamingManager.class);
  }

  @Provides
  TrainerConfigServiceGrpc.TrainerConfigServiceBlockingStub
      providesTrainerConfigServiceBlockingStub(ManagedChannel channel) {
    return TrainerConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  EntityDataServiceConfig providesEntityDataServiceConfig() {
    return new EntityDataServiceConfig(this.config);
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

  @Provides
  ApiNamingConfig providesApiNamingConfig() {
    return new ApiNamingConfig(this.config);
  }

  private MultiFileSystemModelStoreConfig getModelStoreConfig(Config config) {
    List<? extends Config> stores = config.getConfigList(MODEL_STORES_CONFIG_PATH);
    MultiFileSystemModelStoreConfig.Builder configBuilder =
        MultiFileSystemModelStoreConfig.Builder.builder();
    LOGGER.info("building config for {} file store", stores.size());
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
