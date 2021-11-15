package ai.traceable.localprocessing.config.service.ruleservice;

import ai.traceable.localprocessing.config.service.LocalProcessingConfigServiceConfig;
import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinatorModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class LocalProcessingRulesServiceModule extends AbstractModule {
  private final ManagedChannel channel;
  private final Config config;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  public LocalProcessingRulesServiceModule(
      ManagedChannel channel,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.config = config;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(LocalProcessingRulesServiceImpl.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    install(new ConfigServiceCoordinatorModule());
  }

  @Provides
  LocalProcessingConfigServiceConfig providesLocalProcessingConfigServiceConfig() {
    return new LocalProcessingConfigServiceConfig(this.config);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
