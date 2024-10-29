package ai.traceable.saved.query.config.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class SavedQueryConfigServiceModule extends AbstractModule {
  private final Config config;
  private final Channel channel;
  private final ConfigChangeEventGenerator changeEventGenerator;

  SavedQueryConfigServiceModule(
      Config config, Channel channel, ConfigChangeEventGenerator changeEventGenerator) {
    this.config = config;
    this.channel = channel;
    this.changeEventGenerator = changeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(BindableService.class).to(SavedQueryConfigServiceImpl.class);
    bind(Config.class).toInstance(config);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
