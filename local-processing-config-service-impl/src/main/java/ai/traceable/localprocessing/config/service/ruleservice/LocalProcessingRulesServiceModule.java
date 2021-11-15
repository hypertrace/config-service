package ai.traceable.localprocessing.config.service.ruleservice;

import ai.traceable.localprocessing.config.service.LocalProcessingConfigServiceConfig;
import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinatorModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;

public class LocalProcessingRulesServiceModule extends AbstractModule {
  private final ManagedChannel channel;
  private final Config config;

  public LocalProcessingRulesServiceModule(ManagedChannel channel, Config config) {
    this.channel = channel;
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(LocalProcessingRulesServiceImpl.class);
    bind(ManagedChannel.class).toInstance(channel);
    install(new ConfigServiceCoordinatorModule());
  }

  @Provides
  LocalProcessingConfigServiceConfig providesLocalProcessingConfigServiceConfig() {
    return new LocalProcessingConfigServiceConfig(this.config);
  }
}
