package ai.traceable.localprocessing.config.service;

import ai.traceable.localprocessing.config.service.apinaming.ApiNamingManagerModule;
import ai.traceable.localprocessing.config.service.config.ApiNamingConfig;
import ai.traceable.localprocessing.config.service.config.EntityDataServiceConfig;
import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinatorModule;
import ai.traceable.localprocessing.config.service.customsignature.CustomModsecDetectionManagerModule;
import ai.traceable.localprocessing.config.service.regularmodsec.RegularModsecDetectionManagerModule;
import ai.traceable.platform.apientity.http.model.TrieModel;
import ai.traceable.platform.model.store.ModelPersistentStore;
import ai.traceable.platform.model.store.MultiFileSystemModelStore;
import ai.traceable.platform.model.store.config.MultiFileSystemModelStoreConfig;
import ai.traceable.platform.model.store.config.MultiFileSystemModelStoreConfig.Builder;
import ai.traceable.platform.model.store.config.MultiFileSystemModelStoreConfig.FileSystemModelStoreConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalProcessingConfigServiceModule extends AbstractModule {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(LocalProcessingConfigServiceModule.class);
  private final ManagedChannel channel;
  private final Config config;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private static final String INPUT_DATA_CONFIG_PATH = "input.data";
  private static final String MODEL_STORES_CONFIG_PATH = "model.stores";
  private static final String MODEL_STORES_READ_ENABLED_FLAG = "read.enabled";
  private static final String MODEL_STORES_READ_PRIORITY_PATH = "read.priority";
  private static final String MODEL_STORES_WRITE_DISABLED_PATH = "write.disabled";

  public LocalProcessingConfigServiceModule(
      ManagedChannel channel,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.config = config;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(LocalProcessingConfigServiceImpl.class);
    bind(ManagedChannel.class).toInstance(channel);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    install(new ConfigServiceCoordinatorModule());
    install(new CustomModsecDetectionManagerModule());
    install(new RegularModsecDetectionManagerModule());
    install(new ApiNamingManagerModule());
  }

  @Provides
  LocalProcessingConfigServiceConfig providesCustomSignatureServiceConfig() {
    return new LocalProcessingConfigServiceConfig(this.config);
  }

  @Provides
  EntityDataServiceConfig providesEntityDataServiceConfig() {
    return new EntityDataServiceConfig(this.config);
  }

  @Provides
  @Singleton
  public ModelPersistentStore<TrieModel> provideModelStore() {
    Config inputDataConfig = this.config.getConfig(INPUT_DATA_CONFIG_PATH);
    ModelPersistentStore<TrieModel> modelStore =
        new MultiFileSystemModelStore<>(TrieModel.class, getModelStoreConfig(inputDataConfig));
    modelStore.init(inputDataConfig);
    return modelStore;
  }

  @Provides
  ApiNamingConfig providesApiNamingConfig() {
    return new ApiNamingConfig(this.config);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  private MultiFileSystemModelStoreConfig getModelStoreConfig(Config config) {
    List<? extends Config> stores = config.getConfigList(MODEL_STORES_CONFIG_PATH);
    Builder configBuilder = MultiFileSystemModelStoreConfig.Builder.builder();
    LOGGER.info("building config for {} file store", stores.size());
    for (Config conf : stores) {
      configBuilder.addConfig(
          FileSystemModelStoreConfig.Builder.builder()
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
