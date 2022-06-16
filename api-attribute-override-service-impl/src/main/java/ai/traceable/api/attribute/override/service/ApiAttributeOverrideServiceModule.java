package ai.traceable.api.attribute.override.service;

import ai.traceable.api.attribute.override.service.v1.ApiAttributeOverrideServiceGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class ApiAttributeOverrideServiceModule extends AbstractModule {
  private final ManagedChannel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  ApiAttributeOverrideServiceModule(
      ManagedChannel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(ApiAttributeOverrideServiceImpl.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(ManagedChannel.class).toInstance(channel);
  }

  @Provides
  ApiAttributeOverrideServiceGrpc.ApiAttributeOverrideServiceBlockingStub
      providesAttributeOverrideService() {
    return ApiAttributeOverrideServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub providesConfigService() {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
