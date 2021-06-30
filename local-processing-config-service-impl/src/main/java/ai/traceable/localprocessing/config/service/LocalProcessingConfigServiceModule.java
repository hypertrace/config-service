package ai.traceable.localprocessing.config.service;

import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinatorModule;
import ai.traceable.localprocessing.config.service.customsignature.CustomModsecDetectionManagerModule;
import ai.traceable.localprocessing.config.service.regularmodsec.RegularModsecDetectionManagerModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;

public class LocalProcessingConfigServiceModule extends AbstractModule {
  private final ManagedChannel channel;
  private final Config config;

  public LocalProcessingConfigServiceModule(ManagedChannel channel, Config config) {
    this.channel = channel;
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(LocalProcessingConfigServiceImpl.class);
    bind(ManagedChannel.class).toInstance(channel);
    install(new ConfigServiceCoordinatorModule());
    install(new CustomModsecDetectionManagerModule());
    install(new RegularModsecDetectionManagerModule());
  }

  @Provides
  LocalProcessingConfigServiceConfig providesCustomSignatureServiceConfig() {
    return new LocalProcessingConfigServiceConfig(this.config);
  }
}
