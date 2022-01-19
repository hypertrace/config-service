package ai.traceable.localprocessing.config.service;

import ai.traceable.localprocessing.config.service.apinaming.ApiNamingManagerModule;
import ai.traceable.localprocessing.config.service.config.ApiNamingConfig;
import ai.traceable.localprocessing.config.service.config.EntityDataServiceConfig;
import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinatorModule;
import ai.traceable.localprocessing.config.service.customsignature.CustomModsecDetectionManagerModule;
import ai.traceable.localprocessing.config.service.regularmodsec.RegularModsecDetectionManagerModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class LocalProcessingConfigServiceModule extends AbstractModule {
  private final ManagedChannel channel;
  private final Config config;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

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
  ApiNamingConfig providesDefaultApiNamingConfig() {
    return new ApiNamingConfig(this.config);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
